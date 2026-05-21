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
    KeepAliveInterval = TimeSpan.FromSeconds(20)
});

app.MapGet("/", () => Results.Json(new
{
    ok = true,
    service = "rola-gateway-api",
    version = "1.0.0"
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
        // Faz 1: hard-coded demo.
        // Faz 2: PostgreSQL + Redis cache yapacağız.
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

        RobotHelloMessage? hello = await ReceiveHelloAsync(robotSocket, cancellationToken);

        if (hello is null || !string.Equals(hello.Type, "hello", StringComparison.OrdinalIgnoreCase))
        {
            await SendJsonAsync(robotSocket, new
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

        await SendJsonAsync(robotSocket, new
        {
            type = "hello.accepted",
            deviceId = hello.DeviceId,
            tenantCode = tenant.TenantCode,
            assistantName = tenant.AssistantName,
            serverTimeUtc = DateTimeOffset.UtcNow
        }, cancellationToken);

        await bridge.RunAsync(robotSocket, hello, tenant, logger, cancellationToken);
    }

    private static async Task<RobotHelloMessage?> ReceiveHelloAsync(
        WebSocket socket,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);

        try
        {
            var result = await socket.ReceiveAsync(
                buffer.AsMemory(0, buffer.Length),
                cancellationToken);

            if (result.MessageType != WebSocketMessageType.Text)
                return null;

            var json = Encoding.UTF8.GetString(buffer, 0, result.Count);

            return JsonSerializer.Deserialize<RobotHelloMessage>(
                json,
                JsonOptions.Default);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static Task SendJsonAsync(
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

public sealed class OpenAiRealtimeBridge
{
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

        var uri = new Uri("wss://api.openai.com/v1/realtime?model=gpt-realtime");

        await openAiSocket.ConnectAsync(uri, cancellationToken);

        logger.LogInformation("[OpenAI] connected");

        var fromOpenAi = PumpOpenAiToRobotAsync(
            openAiSocket,
            robotSocket,
            tenant,
            logger,
            cancellationToken);

        var fromRobot = PumpRobotToOpenAiAsync(
            robotSocket,
            openAiSocket,
            logger,
            cancellationToken);

        await Task.WhenAny(fromOpenAi, fromRobot);

        try
        {
            if (robotSocket.State == WebSocketState.Open)
                await robotSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "session ended", CancellationToken.None);
        }
        catch { }

        try
        {
            if (openAiSocket.State == WebSocketState.Open)
                await openAiSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "session ended", CancellationToken.None);
        }
        catch { }
    }

    private static async Task PumpOpenAiToRobotAsync(
        ClientWebSocket openAiSocket,
        WebSocket robotSocket,
        TenantConfig tenant,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(256 * 1024);

        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   openAiSocket.State == WebSocketState.Open &&
                   robotSocket.State == WebSocketState.Open)
            {
                var result = await openAiSocket.ReceiveAsync(
                    buffer.AsMemory(0, buffer.Length),
                    cancellationToken);

                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                var json = Encoding.UTF8.GetString(buffer, 0, result.Count);

                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                var type = root.TryGetProperty("type", out var typeEl)
                    ? typeEl.GetString()
                    : null;

                if (type is null)
                    continue;

                logger.LogInformation("[OpenAI EVENT] {Type}", type);

                if (type == "session.created")
                {
                    await SendSessionUpdateAsync(openAiSocket, tenant, cancellationToken);
                    continue;
                }

                if (type == "session.updated")
                {
                    await SendJsonAsync(robotSocket, new
                    {
                        type = "gateway.ready"
                    }, cancellationToken);

                    continue;
                }

                if (type == "error")
                {
                    await SendJsonAsync(robotSocket, new
                    {
                        type = "gateway.error",
                        source = "openai",
                        payload = root
                    }, cancellationToken);

                    continue;
                }

                if (type == "response.output_audio.delta" &&
                    root.TryGetProperty("delta", out var deltaEl))
                {
                    var base64 = deltaEl.GetString();

                    if (!string.IsNullOrWhiteSpace(base64))
                    {
                        var pcm = Convert.FromBase64String(base64);

                        await robotSocket.SendAsync(
                            pcm,
                            WebSocketMessageType.Binary,
                            true,
                            cancellationToken);
                    }

                    continue;
                }

                if (type == "response.output_audio.done")
                {
                    await SendJsonAsync(robotSocket, new
                    {
                        type = "audio.done"
                    }, cancellationToken);

                    continue;
                }

                if (type.StartsWith("input_audio_buffer.", StringComparison.Ordinal) ||
                    type.StartsWith("response.", StringComparison.Ordinal))
                {
                    await SendJsonAsync(robotSocket, new
                    {
                        type
                    }, cancellationToken);
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task PumpRobotToOpenAiAsync(
        WebSocket robotSocket,
        ClientWebSocket openAiSocket,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(128 * 1024);

        try
        {
            while (!cancellationToken.IsCancellationRequested &&
                   robotSocket.State == WebSocketState.Open &&
                   openAiSocket.State == WebSocketState.Open)
            {
                var result = await robotSocket.ReceiveAsync(
                    buffer.AsMemory(0, buffer.Length),
                    cancellationToken);

                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var text = Encoding.UTF8.GetString(buffer, 0, result.Count);

                    // Robot şu an OpenAI formatında JSON gönderebilir:
                    // input_audio_buffer.append vb.
                    await openAiSocket.SendAsync(
                        Encoding.UTF8.GetBytes(text),
                        WebSocketMessageType.Text,
                        true,
                        cancellationToken);

                    continue;
                }

                if (result.MessageType == WebSocketMessageType.Binary)
                {
                    // Faz 2'de Opus binary protokolünü burada işleyeceğiz.
                    // Şimdilik binary robot input'u OpenAI'ye direkt gönderilmiyor.
                    logger.LogDebug("[Robot] binary audio received length={Length}", result.Count);
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
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
                            threshold = 0.5,
                            prefix_padding_ms = 300,
                            silence_duration_ms = 700,
                            create_response = true,
                            interrupt_response = true
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

        await SendJsonAsync(socket, payload, cancellationToken);
    }

    private static Task SendJsonAsync(
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