using System;
using System.Threading;
using RepProc4Replenit.Core;
using RepProc4Replenit.Modules.Server;
using RepProc4Replenit.Modules.Worker;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            if (args.Length < 1)
            {
                Console.WriteLine("[ERROR]: Mode not specified!");
                return;
            }
 
            string programMode = args[0];
        
            if (programMode.ToLower() == "server" && args.Length < 2)
            {
                Console.WriteLine("[ERROR]: Csv file not specified!");
                return;
            }
        
        
            Console.WriteLine($"\n\n\nRepProc4Replenit Program Started\n\n\n");
            Console.WriteLine($"[INFO]: Program Mode: {programMode}");

            Console.CancelKeyPress += (sender, eventArgs) =>
            { 
                RuntimeControl.ProgramCancellation.Cancel();
                eventArgs.Cancel = true;
            };
            
            await RuntimeControl.Start(programMode);

            if (programMode.ToLower() == "server")
                await ServerControl.Run(args[1]);
        
            if (programMode.ToLower() == "consumer")
                WorkerControl.Run();

            if (programMode.ToLower() == "dualmode")
            {
                Task.Run(() => WorkerControl.Run());
                await ServerControl.Run(args[1]);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[FATAL ERROR]: {ex.Message}");
            Console.WriteLine($"[FATAL ERROR]: {ex.StackTrace}");
        }
    }
}