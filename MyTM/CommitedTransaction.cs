using System;
using System.Collections.Generic;
using System.Threading;
using TP;

namespace MyTM
{
    [Serializable]
    public class CommitedTransaction
    {
        private const int maxRetry = 3; 
        private static int expBackoff = 500; // exponential back off starts from 500ms
        private static int stepTimeout = 50000; // millisecond
        private List<CommitState> rmCommitStates;
        private AutoResetEvent stepWaitEvent = new AutoResetEvent(false);
        public string Message = string.Empty; // This is for demo 
        public CommitedTransaction(Transaction context, ResourceManagerList rms)
        {
            this.ResouceManagers = rms;
            this.Context = context;
            this.State = CommitState.None;
            this.rmCommitStates = new List<CommitState>();
            for (int i = 0; i < rms.ResourceManagers.Count; i++)
            {
                rmCommitStates.Add(CommitState.None);
            }
        }

        public AutoResetEvent DoneEvent = new AutoResetEvent(false);
        public CommitState State
        {
            get;
            set;
        }

        public Transaction Context { get; private set; }
        public ResourceManagerList ResouceManagers { get; private set; }

        public override string ToString()
        {
            string result = string.Empty;
            string rmstring = string.Empty;
            foreach (RM r in ResouceManagers.ResourceManagers)
            {
                rmstring += r.GetName() + ",";
            }
            result += string.Format("{0}\t{1}\t{2}", Context.Id, State, rmstring);
            return result;
        }

        public static CommitedTransaction FromString(string input)
        {
            string[] items = input.Split(new char[] { '\t' }, StringSplitOptions.None);
            Transaction trans = new Transaction();
            trans.Id = new Guid(items[0]);
            string[] rms = items[2].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            ResourceManagerList rml = new ResourceManagerList();
            foreach (string s in rms)
            {
                RM rm = new MyRM.MyRM();
                rm.SetName(s);
                rml.Add(rm);
            }

            CommitedTransaction result = new CommitedTransaction(trans, rml);
            switch (items[1])
            {
                case "None":
                    result.State = CommitState.None;
                    break;
                case "Committed":
                    result.State = CommitState.Committed;
                    break;
                case "Prepared":
                    result.State = CommitState.Prepared;
                    break;
                case "Done":
                    result.State = CommitState.Done;
                    break;
            }

            return result;
        }
        /// <summary>
        /// Sets state of a RM.
        /// If all RMs are set to a state. the stepWaitEvent will be set. 
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <param name="state"></param>
        public void SetState(int index, CommitState state)
        {
            CommitState result = CommitState.Rollbacked;
            lock (rmCommitStates)
            {
                rmCommitStates[index] = state;

                foreach (CommitState item in this.rmCommitStates)
                {
                    if (item < result)
                    {
                        result = item;
                    }
                }

                if (result != this.State)
                {
                    this.State = result;
                    this.stepWaitEvent.Set();
                }
            }
        }

        public delegate void CommitAction();

        /// <summary>
        /// The method to do two phrase commit
        /// This method shall be hosted in a new thread. 
        /// The caller can wait on the DoneEvent. 
        /// </summary>
        public void StartCommit()
        {
            bool isStepComplete = WaitStepWithExpBackoff(Prepare);

            if (isStepComplete)
            {
                TwoPhaseCommit.WriteLog();
                isStepComplete = WaitStepWithExpBackoff(Commit);

                if (isStepComplete)
                {
                    TwoPhaseCommit.WriteLog();
                    DoneEvent.Set();
                    TwoPhaseCommit.DoneCommit(this.Context);
                }
                else
                {
                    Message += string.Format("2PC:Retry Commit {0}\r", this.Context.Id);
                    WaitStepWithExpBackoff(Commit);
                }
            }
            else
            {
                // Presume abort
                Rollback();
            }
        }


        /// <summary>
        /// TM shall call this function when it restarts. 
        /// if return is true
        ///     1. TM shall delete the transaction or retry commit if the state after recover is Rollbacked.
        ///     2. TM shall delete the transaction if the state after recover is done;
        /// if return is false
        ///     TM shall retry the recover process;
        /// </summary>
        /// <return>false if the recover process failed.</return>
        public bool Recover()
        {
            switch (State)
            {
                case CommitState.Committed:
                    Rollback();
                    TwoPhaseCommit.WriteLog();
                    break;
                case CommitState.Prepared:
                    Commit();
                    TwoPhaseCommit.WriteLog();
                    break;
                default:
                    this.stepWaitEvent.Set();
                    break;
            }

            return this.stepWaitEvent.WaitOne(stepTimeout);
        }
        
        private void Prepare()
        {
            var list = this.ResouceManagers.ResourceManagers;
            for (int i = 0; i < list.Count; i++)
            {
                RM r = list[i];
                int temp = i;
                Message += string.Format("2PC:Prepare {0}:{1}\r", this.Context.Id, r.GetName());
                ThreadPool.QueueUserWorkItem(o =>
                {
                    XaResponse response = r.Prepare(this.Context);
                    if (response == XaResponse.XA_OK)
                    {
                        this.SetState(temp, CommitState.Prepared);
                    }
                });
            }
        }

        private void Commit()
        {
            var list = this.ResouceManagers.ResourceManagers;
            for (int i = 0; i < list.Count; i++)
            {
                RM r = list[i];
                Message += string.Format("2PC:Commit {0}:{1}\r", this.Context.Id, r.GetName());
                int temp = i;
                ThreadPool.QueueUserWorkItem(o =>
                {
                    XaResponse response = r.Commit(this.Context);
                    if (response == XaResponse.XA_OK)
                    {
                        this.SetState(temp, CommitState.Done);
                    }
                });
            }
        }

        private void Rollback()
        {
            var list = this.ResouceManagers.ResourceManagers;
            for (int i = 0; i < list.Count; i++)
            {
                RM r = list[i];
                Message += string.Format("2PC:Rollback {0}:{1}\r", this.Context.Id, r.GetName());
                int temp = i;
                ThreadPool.QueueUserWorkItem(o =>
                {
                    XaResponse response = r.Abort(this.Context);
                    if (response == XaResponse.XA_OK)
                    {
                        this.SetState(temp, CommitState.Rollbacked);
                    }
                });
            }
        }

        private bool WaitStepWithExpBackoff(CommitAction action)
        {
            int retry = 0;
            int sleeptime = expBackoff;
            bool isStepComplete = false;
            action();
            while (!(isStepComplete = this.stepWaitEvent.WaitOne(stepTimeout)) && retry < maxRetry)
            {
                Thread.Sleep(sleeptime);
                sleeptime *= 2;
                retry++;
                Message += string.Format("Sleep and retry {0}\r", retry);
                action();
            }

            return isStepComplete;
        }
    }

}
