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
        public static void CleanUp2pc()
        {
            File.Delete(TwoPhaseCommit.LogFileName);
            TwoPhaseCommit_Accessor.Cleanup();
        }

        public static void CleanUpDbFiles()
        {
            string[] files = Directory.GetFiles(Environment.CurrentDirectory, "*.data.0");
            foreach (string file in files)
            {
                File.Delete(file);
            }
            files = Directory.GetFiles(Environment.CurrentDirectory, "*.data.1");
            foreach (string file in files)
            {
                File.Delete(file);
            }
            files = Directory.GetFiles(Environment.CurrentDirectory, "*.index");
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
            CommonFunction.CleanUp2pc();
            CommonFunction.CleanUpDbFiles();
        }
    }
}
