open System
open System.Threading
open Serilog
open Confluent.Kafka

[<EntryPoint>]
let main argv =
    let logger =
        LoggerConfiguration()
            .WriteTo.Console()
            .CreateLogger()

    if argv.Length <> 2 then
        logger.Error
            "Informe 2 parâmetros: no primeiro o IP/porta para testes com o Kafka, no segundo o Topic a ser utilizado no consumo das mensagens..."

        -1
    else
        let bootstrapServers = argv.[0]
        let nomeTopic = argv.[1]

        logger.Information $"BootstrapServers = {bootstrapServers}"
        logger.Information $"Topic = {nomeTopic}"

        let config =
            ConsumerConfig(BootstrapServers = bootstrapServers, GroupId = $"{nomeTopic}-group-0", AutoOffsetReset = AutoOffsetReset.Earliest)
        
        let cts = new CancellationTokenSource()
        Console.CancelKeyPress.Add (fun e ->
            e.Cancel <- true
            cts.Cancel())
        
        try
            use consumer = ConsumerBuilder<Ignore, string>(config).Build()
            consumer.Subscribe(nomeTopic)

            try
                while true do
                    let cr = consumer.Consume(cts.Token)
                    logger.Information $"Mensagem lida: {cr.Message.Value}"
                0
            with
            | :? OperationCanceledException ->
                consumer.Close()
                logger.Warning "Cancelando a execução do consumer..."
                0
        with
        | ex ->
            logger.Error $"Exceção: {ex.GetType().FullName} | Mensagem: {ex.Message}"
            -2
