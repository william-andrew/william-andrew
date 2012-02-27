﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using TP;
using System.Runtime.Serialization;
using MyRM;
namespace MyTM
{
    [Serializable]
    public class CommitedTransaction
    {
        private static int stepTimeout = 50000; // millisecond
        private List<CommitState> rmCommitStates;
        private AutoResetEvent stepWaitEvent = new AutoResetEvent(false);
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

        /// <summary>
        /// The method to do two phrase commit
        /// This method shall be hosted in a new thread. 
        /// The caller can wait on the DoneEvent. 
        /// </summary>
        public void StartCommit()
        {
            Prepare();

            if (this.stepWaitEvent.WaitOne(stepTimeout))
            {
                TwoPhraseCommit.WriteLog();
                Commit();

                if (this.stepWaitEvent.WaitOne(stepTimeout))
                {
                    TwoPhraseCommit.WriteLog();
                    DoneEvent.Set();
                    TwoPhraseCommit.DoneCommit(this.Context);
                }
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
                    TwoPhraseCommit.WriteLog();
                    break;
                case CommitState.Prepared:
                    Commit();
                    TwoPhraseCommit.WriteLog();
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
    }

}