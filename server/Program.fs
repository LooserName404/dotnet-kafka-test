open System
open System.Threading.Tasks
open Serilog
open Confluent.Kafka

[<EntryPoint>]
let main argv =
    let logger =
        LoggerConfiguration()
            .WriteTo.Console()
            .CreateLogger()

    logger.Information "Testando o envio de mensagens com Kafka."

    if argv.Length < 3 then
        logger.Error "Informe ao menos 3 parâmetros: no primeiro o IP/porta para testes com o Kafka, no segundo o Topic que receberá a mensagem, já no terceiro em diante as mensagens a serem enviadas a um Topic no Kafka..."
        -1
    else
        let bootstrapServers = argv.[0]
        let nomeTopic = argv.[1]

        logger.Information $"BootstrapServers = %s{bootstrapServers}"
        logger.Information $"Topic = %s{nomeTopic}"

        try
            let config = ProducerConfig(BootstrapServers = bootstrapServers)
            use producer = ProducerBuilder<Null, string>(config).Build()

            argv
            |> Seq.skip 2
            |> Seq.filter (fun s -> String.IsNullOrWhiteSpace s |> not)
            |> Seq.map (fun arg -> async {
                    printfn "%s" arg
                    let! result = producer.ProduceAsync(nomeTopic, Message(Value = arg)) |> Async.AwaitTask
                    printfn "%O" result
                    logger.Information $"Mensagem: {arg} | Status: {result.Status.ToString()}"
                })
            |> Async.Parallel
            |> Async.RunSynchronously
            |> ignore

            logger.Information "Concluído o envio de mensagens"
            0
        with
        | ex ->
            logger.Error $"Exceção: {ex.GetType().FullName} | Mensagem: {ex.Message}"
            -2
