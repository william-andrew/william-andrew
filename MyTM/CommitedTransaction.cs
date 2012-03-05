using System;
using System.Collections.Generic;
using System.Threading;
using TP;
using System.Net;
using System.Diagnostics;

namespace MyTM
{
    [Serializable]
    public class CommitedTransaction
    {
        private const int maxRetry = 3; 
        private static int expBackoff = 500; // exponential back off starts from 500ms
        private static int stepTimeout = 2000; // millisecond
        private List<CommitState> rmCommitStates;
        private AutoResetEvent stepWaitEvent = new AutoResetEvent(false);
        private bool prepareFail; // exit before all RM is prepared (send Prepare to 1 RM then killed)
        private bool commitFail; //exit before all RM are done (send commited to 1 RM then killed)
        public string Message = string.Empty;
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
            foreach (var r in ResouceManagers.RMNames)
            {
                rmstring += r + ",";
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
        private void SetState(int index, CommitState state)
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
        public void StartCommit(bool prepareFail = false, bool commitFail = false)
        {
            this.prepareFail = prepareFail;
            this.commitFail = commitFail;
            bool isStepComplete = false;
            var list = this.ResouceManagers.ResourceManagers;
            for (int i = 0; i < list.Count; i++)
            {
                rmCommitStates[i] = CommitState.Committed;
            }

            TwoPhaseCommit.WriteLog();
            Prepare();

            if ((isStepComplete = this.stepWaitEvent.WaitOne(stepTimeout)))
            {
                TwoPhaseCommit.WriteLog();
                isStepComplete = WaitStepWithExpBackoff(Commit);

                if (isStepComplete)
                {
                    TwoPhaseCommit.WriteLog();
                    TwoPhaseCommit.DoneCommit(this.Context);
                    DoneEvent.Set();
                }
                else
                {
                    string outstring = string.Format("2PC:Retry Commit {0}\r", this.Context.Id);
                    this.Message += outstring;
                    Console.Write(string.Format("2PC:Retry Commit {0}\r", this.Context.Id));
                    WaitStepWithExpBackoff(Commit);
                }
            }
            else
            {
                // Presume abort
                Rollback();
                TwoPhaseCommit.WriteLog();
                TwoPhaseCommit.DoneCommit(this.Context);
                DoneEvent.Set();
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

        private void SelfDestroying()
        {
            Process.GetCurrentProcess().Kill();
        }

        private void Prepare()
        {
            var list = this.ResouceManagers.ResourceManagers;
            for (int i = 0; i < list.Count; i++)
            {
                RM r = list[i];
                int temp = i;
                string outstring = string.Format("2PC:Prepare {0}:{1}\r", this.Context.Id, this.ResouceManagers.RMNames[temp]);
                this.Message += outstring;
                Console.Write(outstring);
                ThreadPool.QueueUserWorkItem(o =>
                {
                    try
                    {
                        XaResponse response = r.Prepare(this.Context);
                        if (response == XaResponse.XA_OK)
                        {
                            this.SetState(temp, CommitState.Prepared);
                        }
                    }
                    catch (WebException)
                    {
                    }
                });

                if (prepareFail)
                {
                    SelfDestroying();
                }
            }
        }

        private void Commit()
        {
            var list = this.ResouceManagers.ResourceManagers;
            for (int i = 0; i < list.Count; i++)
            {
                RM r = list[i];
                int temp = i;
                string outstring = string.Format("2PC:Commit {0}:{1}\r", this.Context.Id, this.ResouceManagers.RMNames[temp]);
                this.Message += outstring;
                Console.Write(outstring);
                ThreadPool.QueueUserWorkItem(o =>
                {

                    try
                    {
                        XaResponse response = r.Commit(this.Context);
                        if (response == XaResponse.XA_OK)
                        {
                            this.SetState(temp, CommitState.Done);
                        }
                    }
                    catch (WebException)
                    {
                    }
                });

                if (commitFail)
                {
                    SelfDestroying();
                }
            }
        }

        private void Rollback()
        {
            var list = this.ResouceManagers.ResourceManagers;
            for (int i = 0; i < list.Count; i++)
            {
                RM r = list[i];
                string outstring = string.Format("2PC:Rollback {0}:{1}\r", this.Context.Id, this.ResouceManagers.RMNames[i]);
                this.Message += outstring;
                Console.Write(outstring);
                int temp = i;
                ThreadPool.QueueUserWorkItem(o =>
                {

                    try
                    {
                        r.Abort(this.Context);
                    }
                    catch (WebException)
                    {
                    }
                    // presume abort, if we don't check return of the abort 
                    // Once the abort message is sent, it is consider done.
                    this.SetState(temp, CommitState.Rollbacked);
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
                string outstring = string.Format("Sleep and retry {0}\r", retry);
                this.Message += outstring;
                Console.Write(outstring);
                action();
            }

            return isStepComplete;
        }
    }

}
