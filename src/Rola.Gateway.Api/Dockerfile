FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Rola.Gateway.Api.csproj .
RUN dotnet restore

COPY . .
RUN dotnet publish -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

ENV ASPNETCORE_URLS=http://+:3000
ENV DOTNET_EnableDiagnostics=0
ENV DOTNET_GCServer=1

COPY --from=build /app/publish .

EXPOSE 3000

ENTRYPOINT ["dotnet", "Rora.Gateway.Api.dll"]