using System.Buffers;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.UseUrls(
    Environment.GetEnvironmentVariable("ASPNETCORE_URLS")
    ?? $"http://+:{Environment.GetEnvironmentVariable("PORT") ?? "3000"}");

builder.Services.AddSingleton<TenantConfigService>();
builder.Services.AddSingleton<OpenAiRealtimeBridge>();

var app = builder.Build();

app.UseWebSockets(new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromSeconds(15)
});

app.MapGet("/", () => Results.Json(new
{
    ok = true,
    service = "rola-gateway-api",
    version = "1.0.2"
}));

app.MapGet("/health", () => Results.Json(new
{
    ok = true,
    service = "rola-gateway-api",
    utc = DateTimeOffset.UtcNow
}));

app.Map("/robot/ws", async context =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = StatusCodes.Status400BadRequest;
        await context.Response.WriteAsync("WebSocket required");
        return;
    }

    using var robotSocket = await context.WebSockets.AcceptWebSocketAsync();

    var logger = context.RequestServices
        .GetRequiredService<ILoggerFactory>()
        .CreateLogger("RobotWs");

    var tenantService = context.RequestServices.GetRequiredService<TenantConfigService>();
    var bridge = context.RequestServices.GetRequiredService<OpenAiRealtimeBridge>();

    await RobotSessionHandler.HandleAsync(
        robotSocket,
        tenantService,
        bridge,
        logger,
        context.RequestAborted);
});

app.Run();

public sealed record RobotHelloMessage(
    string Type,
    string DeviceId,
    string TenantCode,
    string? FirmwareVersion,
    RobotAudioOptions? Audio);

public sealed record RobotAudioOptions(
    string Format,
    int SampleRate,
    int Channels,
    int FrameMs);

public sealed record TenantConfig(
    string TenantCode,
    string CompanyName,
    string Sector,
    string AssistantName,
    string LanguageMode,
    string LogLanguage,
    string[] AllowedTopics,
    string[] BlockedTopics,
    string KnowledgeText);

public sealed class TenantConfigService
{
    public TenantConfig Resolve(string tenantCode)
    {
        return tenantCode switch
        {
            "demo-gelinlikci" => new TenantConfig(
                TenantCode: "demo-gelinlikci",
                CompanyName: "Rola Bridal",
                Sector: "Gelinlik Mağazası",
                AssistantName: "Rola",
                LanguageMode: "detect",
                LogLanguage: "tr",
                AllowedTopics:
                [
                    "gelinlik modelleri",
                    "randevu",
                    "prova",
                    "fiyat aralığı",
                    "kumaş",
                    "teslim süresi",
                    "aksesuar"
                ],
                BlockedTopics:
                [
                    "siyaset",
                    "din",
                    "sağlık teşhisi",
                    "hukuki tavsiye",
                    "sektör dışı sohbet"
                ],
                KnowledgeText:
                """
                Rola Bridal İstanbul'da gelinlik danışmanlığı verir.
                Ürünler:
                1. Helen Model: A kesim, Fransız dantel, fiyat aralığı 45.000-60.000 TL, teslim 30 gün.
                2. Mira Model: Prenses kesim, taş işlemeli, fiyat aralığı 65.000-85.000 TL, teslim 45 gün.
                3. Ela Model: Sade saten, modern kesim, fiyat aralığı 35.000-50.000 TL, teslim 25 gün.
                Randevu gereklidir. Net fiyat prova sonrası verilir.
                """
            ),

            _ => new TenantConfig(
                TenantCode: tenantCode,
                CompanyName: "Demo İşletme",
                Sector: "Genel Demo",
                AssistantName: "Rola",
                LanguageMode: "detect",
                LogLanguage: "tr",
                AllowedTopics: ["işletme bilgileri", "ürünler", "randevu"],
                BlockedTopics: ["siyaset", "din", "hukuk", "sağlık teşhisi"],
                KnowledgeText: "Bu demo tenant bilgisidir."
            )
        };
    }
}

public static class RobotSessionHandler
{
    public static async Task HandleAsync(
        WebSocket robotSocket,
        TenantConfigService tenantService,
        OpenAiRealtimeBridge bridge,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        logger.LogInformation("[Robot] connected");

        var helloJson = await WebSocketJsonHelper.ReceiveFullTextAsync(
            robotSocket,
            cancellationToken);

        if (string.IsNullOrWhiteSpace(helloJson))
        {
            logger.LogWarning("[Robot] empty hello");
            return;
        }

        RobotHelloMessage? hello;

        try
        {
            hello = JsonSerializer.Deserialize<RobotHelloMessage>(
                helloJson,
                JsonOptions.Default);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[Robot] invalid hello json");

            await WebSocketJsonHelper.SendJsonAsync(robotSocket, new
            {
                type = "error",
                message = "Invalid hello json"
            }, cancellationToken);

            return;
        }

        if (hello is null || !string.Equals(hello.Type, "hello", StringComparison.OrdinalIgnoreCase))
        {
            await WebSocketJsonHelper.SendJsonAsync(robotSocket, new
            {
                type = "error",
                message = "First message must be hello"
            }, cancellationToken);

            return;
        }

        var tenant = tenantService.Resolve(hello.TenantCode);

        logger.LogInformation(
            "[Robot] hello deviceId={DeviceId} tenant={TenantCode} audio={Format}/{SampleRate}",
            hello.DeviceId,
            hello.TenantCode,
            hello.Audio?.Format,
            hello.Audio?.SampleRate);

        await WebSocketJsonHelper.SendJsonAsync(robotSocket, new
        {
            type = "hello.accepted",
            deviceId = hello.DeviceId,
            tenantCode = tenant.TenantCode,
            assistantName = tenant.AssistantName,
            serverTimeUtc = DateTimeOffset.UtcNow
        }, cancellationToken);

        await bridge.RunAsync(robotSocket, hello, tenant, logger, cancellationToken);
    }
}

public sealed class OpenAiRealtimeBridge
{
    private const int AudioBatchTargetBytes = 9_600;
    private readonly string _apiKey;

    public OpenAiRealtimeBridge(IConfiguration configuration)
    {
        _apiKey = configuration["OPENAI_API_KEY"]
            ?? Environment.GetEnvironmentVariable("OPENAI_API_KEY")
            ?? throw new InvalidOperationException("OPENAI_API_KEY is missing");
    }

    public async Task RunAsync(
        WebSocket robotSocket,
        RobotHelloMessage hello,
        TenantConfig tenant,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        using var openAiSocket = new ClientWebSocket();

        openAiSocket.Options.SetRequestHeader("Authorization", $"Bearer {_apiKey}");
        openAiSocket.Options.KeepAliveInterval = TimeSpan.FromSeconds(15);

        var uri = new Uri("wss://api.openai.com/v1/realtime?model=gpt-realtime");

        await openAiSocket.ConnectAsync(uri, cancellationToken);

        logger.LogInformation("[OpenAI] connected");

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var fromOpenAi = PumpOpenAiToRobotAsync(
            openAiSocket,
            robotSocket,
            tenant,
            logger,
            linkedCts.Token);

        var fromRobot = PumpRobotToOpenAiAsync(
            robotSocket,
            openAiSocket,
            logger,
            linkedCts.Token);

        var completed = await Task.WhenAny(fromOpenAi, fromRobot);

        if (completed.IsFaulted)
        {
            logger.LogError(completed.Exception, "[Bridge] pump failed");
        }

        linkedCts.Cancel();

        await SafeCloseAsync(robotSocket, "session ended");
        await SafeCloseAsync(openAiSocket, "session ended");
    }

    private static async Task PumpOpenAiToRobotAsync(
        ClientWebSocket openAiSocket,
        WebSocket robotSocket,
        TenantConfig tenant,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        using var audioBatch = new MemoryStream(32 * 1024);

        while (!cancellationToken.IsCancellationRequested &&
               openAiSocket.State == WebSocketState.Open &&
               robotSocket.State == WebSocketState.Open)
        {
            string? json;

            try
            {
                json = await WebSocketJsonHelper.ReceiveFullTextAsync(
                    openAiSocket,
                    cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (WebSocketException ex)
            {
                logger.LogWarning(ex, "[OpenAI] websocket closed while receiving");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[OpenAI] receive failed");
                break;
            }

            if (string.IsNullOrWhiteSpace(json))
                break;

            using JsonDocument doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var type = root.TryGetProperty("type", out var typeEl)
                ? typeEl.GetString()
                : null;

            if (string.IsNullOrWhiteSpace(type))
                continue;

            logger.LogInformation("[OpenAI EVENT] {Type}", type);

            if (type == "session.created")
            {
                await SendSessionUpdateAsync(openAiSocket, tenant, cancellationToken);
                continue;
            }

            if (type == "session.updated")
            {
                await WebSocketJsonHelper.SendJsonAsync(robotSocket, new
                {
                    type = "gateway.ready"
                }, cancellationToken);

                continue;
            }

            if (type == "error")
            {
                await WebSocketJsonHelper.SendJsonAsync(robotSocket, new
                {
                    type = "gateway.error",
                    source = "openai",
                    message = root.ToString()
                }, cancellationToken);

                continue;
            }

            if (type == "input_audio_buffer.speech_started")
            {
                await WebSocketJsonHelper.SendJsonAsync(robotSocket, new { type }, cancellationToken);
                continue;
            }

            if (type == "input_audio_buffer.speech_stopped")
            {
                await WebSocketJsonHelper.SendJsonAsync(robotSocket, new { type }, cancellationToken);
                continue;
            }

            if (type == "input_audio_buffer.committed")
            {
                await WebSocketJsonHelper.SendJsonAsync(robotSocket, new { type }, cancellationToken);
                continue;
            }

            if (type == "response.created")
            {
                await WebSocketJsonHelper.SendJsonAsync(robotSocket, new { type }, cancellationToken);
                continue;
            }

            if (type == "response.output_audio.delta" &&
                root.TryGetProperty("delta", out var deltaEl))
            {
                var base64 = deltaEl.GetString();

                if (!string.IsNullOrWhiteSpace(base64))
                {
                    var pcm = Convert.FromBase64String(base64);
                    audioBatch.Write(pcm, 0, pcm.Length);

                    if (audioBatch.Length >= AudioBatchTargetBytes)
                    {
                        await FlushAudioBatchAsync(
                            robotSocket,
                            audioBatch,
                            cancellationToken);
                    }
                }

                continue;
            }

            if (type == "response.output_audio.done")
            {
                await FlushAudioBatchAsync(
                    robotSocket,
                    audioBatch,
                    cancellationToken);

                await WebSocketJsonHelper.SendJsonAsync(robotSocket, new
                {
                    type = "audio.done"
                }, cancellationToken);

                continue;
            }

            if (type == "response.done")
            {
                await FlushAudioBatchAsync(
                    robotSocket,
                    audioBatch,
                    cancellationToken);

                await WebSocketJsonHelper.SendJsonAsync(robotSocket, new
                {
                    type = "response.done"
                }, cancellationToken);

                continue;
            }
        }
    }

    private static async Task FlushAudioBatchAsync(
        WebSocket robotSocket,
        MemoryStream audioBatch,
        CancellationToken cancellationToken)
    {
        if (audioBatch.Length <= 0)
            return;

        var buffer = audioBatch.ToArray();

        audioBatch.SetLength(0);

        await robotSocket.SendAsync(
            buffer,
            WebSocketMessageType.Binary,
            true,
            cancellationToken);
    }

    private static async Task PumpRobotToOpenAiAsync(
        WebSocket robotSocket,
        ClientWebSocket openAiSocket,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested &&
               robotSocket.State == WebSocketState.Open &&
               openAiSocket.State == WebSocketState.Open)
        {
            WebSocketMessage message;

            try
            {
                message = await WebSocketJsonHelper.ReceiveFullMessageAsync(
                    robotSocket,
                    cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (WebSocketException ex)
            {
                logger.LogWarning(ex, "[Robot] websocket closed while receiving");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[Robot] receive failed");
                break;
            }

            if (message.MessageType == WebSocketMessageType.Close)
                break;

            if (message.MessageType == WebSocketMessageType.Text)
            {
                await openAiSocket.SendAsync(
                    message.Payload,
                    WebSocketMessageType.Text,
                    true,
                    cancellationToken);

                continue;
            }

            if (message.MessageType == WebSocketMessageType.Binary)
            {
                logger.LogDebug("[Robot] binary input ignored length={Length}", message.Payload.Length);
            }
        }
    }

    private static async Task SendSessionUpdateAsync(
        ClientWebSocket socket,
        TenantConfig tenant,
        CancellationToken cancellationToken)
    {
        var instructions =
            $"""
            Senin adın {tenant.AssistantName}.
            İşletme adı: {tenant.CompanyName}.
            Sektör: {tenant.Sector}.

            Kullanıcının konuştuğu dili algıla ve aynı dilde cevap ver.
            Cevapların kısa, hızlı, doğal ve satış odaklı olsun.
            Sadece aşağıdaki işletme bilgilerine göre cevap ver.
            Sektör dışına çıkma.
            Bilmediğin konuda uydurma; randevu veya yetkiliye yönlendir.
            Her cevapta müşteriyi nazikçe yönlendir.

            İzinli konular:
            {string.Join(", ", tenant.AllowedTopics)}

            Yasak konular:
            {string.Join(", ", tenant.BlockedTopics)}

            İşletme bilgi tabanı:
            {tenant.KnowledgeText}
            """;

        var payload = new
        {
            type = "session.update",
            session = new
            {
                type = "realtime",
                model = "gpt-realtime",
                instructions,
                audio = new
                {
                    input = new
                    {
                        format = new
                        {
                            type = "audio/pcm",
                            rate = 24000
                        },
                        turn_detection = new
                        {
                            type = "server_vad",
                            threshold = 0.55,
                            prefix_padding_ms = 300,
                            silence_duration_ms = 800,
                            create_response = true,
                            interrupt_response = false
                        }
                    },
                    output = new
                    {
                        format = new
                        {
                            type = "audio/pcm",
                            rate = 24000
                        },
                        voice = "shimmer"
                    }
                }
            }
        };

        await WebSocketJsonHelper.SendJsonAsync(socket, payload, cancellationToken);
    }

    private static async Task SafeCloseAsync(WebSocket socket, string reason)
    {
        try
        {
            if (socket.State == WebSocketState.Open ||
                socket.State == WebSocketState.CloseReceived)
            {
                await socket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    reason,
                    CancellationToken.None);
            }
        }
        catch
        {
            // ignored
        }
    }
}

public sealed record WebSocketMessage(
    WebSocketMessageType MessageType,
    byte[] Payload);

public static class WebSocketJsonHelper
{
    public static async Task<string?> ReceiveFullTextAsync(
        WebSocket socket,
        CancellationToken cancellationToken)
    {
        var message = await ReceiveFullMessageAsync(socket, cancellationToken);

        if (message.MessageType == WebSocketMessageType.Close)
            return null;

        if (message.MessageType != WebSocketMessageType.Text)
            return null;

        return Encoding.UTF8.GetString(message.Payload);
    }

    public static async Task<WebSocketMessage> ReceiveFullMessageAsync(
        WebSocket socket,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);

        try
        {
            using var ms = new MemoryStream(64 * 1024);

            while (true)
            {
                var result = await socket.ReceiveAsync(
                    buffer.AsMemory(0, buffer.Length),
                    cancellationToken);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return new WebSocketMessage(WebSocketMessageType.Close, []);
                }

                ms.Write(buffer, 0, result.Count);

                if (result.EndOfMessage)
                {
                    return new WebSocketMessage(
                        result.MessageType,
                        ms.ToArray());
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static Task SendJsonAsync(
        WebSocket socket,
        object payload,
        CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(payload, JsonOptions.Default);
        var bytes = Encoding.UTF8.GetBytes(json);

        return socket.SendAsync(
            bytes,
            WebSocketMessageType.Text,
            true,
            cancellationToken);
    }
}

public static class JsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };
}