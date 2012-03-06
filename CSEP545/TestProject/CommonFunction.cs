using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using MyTM;

namespace TestProject
{
    public static class CommonFunction
    {
        public static void CleanUp2pcLog()
        {
            File.Delete(TwoPhaseCommit.LogFileName);
            TwoPhaseCommit_Accessor.isInitialized = false;
        }

        public static void CleanUpDbFiles()
        {
            string[] files = Directory.GetFiles(Environment.CurrentDirectory, "MYRM_*");
            foreach (string file in files)
            {
                File.Delete(file);
            } 
            files = Directory.GetFiles(Environment.CurrentDirectory, "file*");
            foreach (string file in files)
            {
                File.Delete(file);
            }
            files = Directory.GetFiles(Environment.CurrentDirectory, "*.manifest");
            foreach (string file in files)
            {
                File.Delete(file);
            }
        }

        public static void CleanUpAll()
        {
            CommonFunction.CleanUp2pcLog();
            CommonFunction.CleanUpDbFiles();
        }
    }
}
