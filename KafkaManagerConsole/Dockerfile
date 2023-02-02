FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["KafkaManagerConsole/KafkaManagerConsole.csproj", "KafkaManagerConsole/"]
RUN dotnet restore "KafkaManagerConsole/KafkaManagerConsole.csproj"
COPY . .
WORKDIR "/src/KafkaManagerConsole"
RUN dotnet build "KafkaManagerConsole.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "KafkaManagerConsole.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "KafkaManagerConsole.dll"]
