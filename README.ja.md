# ROSettaDDS

**Pure C#/.NET で実装した、ROS 2 互換 (DDS/RTPS) の通信ライブラリ。**

ネイティブライブラリもブリッジプロセスも必要とせず、ROS 2 ノードと直接 pub/sub できます。
DDS/RTPS のレイヤーから C# で実装しているため、Windows / macOS / Linux に加えて
Android / iOS など IL2CPP・AOT 環境でも動作するクロスプラットフォームを実現します。

[English](README.md) | 日本語

## なぜ ROSettaDDS か

Unity / .NET から ROS 2 と通信する手段は既にいくつか存在します。ROSettaDDS はそれらと
次の点で異なります。

|  | [ros-sharp](https://github.com/siemens/ros-sharp) | [ros2-for-unity](https://github.com/RobotecAI/ros2-for-unity) (ros2cs) | **ROSettaDDS** |
| --- | --- | --- | --- |
| 通信方式 | rosbridge (WebSocket / JSON) | ネイティブ `rcl`/`rmw` バインディング | Pure C# の RTPS/DDS |
| ブリッジプロセス | 必要 (`rosbridge_server`) | 不要 | 不要 |
| ネイティブ依存 | なし | あり (プラットフォーム毎にビルドが必要) | なし |
| 対応プラットフォーム | Unity/.NET が動く所 | ネイティブをビルド済みの環境 (Linux/Windows 中心) | **.NET が動く所** (Win/Mac/Linux/Android/iOS…) |
| ROS 2 と直接 pub/sub | ブリッジ経由 | 直接 | 直接 |
| オーバーヘッド | 大 (JSON シリアライズ + WebSocket) | 小 | 小 |
| AOT / IL2CPP | — | ネイティブ依存に従う | 対応 (msg はコンパイル時生成) |

- *ros-sharp** は ROS 2 側に `rosbridge_server` を立て、WebSocket 経由で JSON をやり取りする
  ブリッジ方式です。導入は容易ですが、ブリッジプロセスが別途必要で、JSON シリアライズと
  WebSocket のオーバーヘッドが乗ります。ROSettaDDS はブリッジを介さず、ROS 2 が話すのと同じ
  RTPS でネイティブに pub/sub します。
- **ros2-for-unity (ros2cs)** は思想が近く、ROS 2 の `rcl`/`rmw` ネイティブライブラリへ C# から
  バインドして直接 DDS pub/sub します。低オーバーヘッドですが、各プラットフォーム向けに
  ネイティブライブラリをビルド・同梱する必要があり、動作環境が限られます。ROSettaDDS は
  DDS/RTPS を C# だけで実装しているためネイティブ依存がなく、macOS やモバイルを含めて
  .NET が動く環境ならどこでも動かせます。

## 特徴

- ROS 2 (Fast DDS) と RTPS レベルで相互通信できる
- `std_msgs` / `builtin_interfaces` / `geometry_msgs` を同梱。`.msg` から CDR 互換な C# 型を生成できる
- Reliable / Best Effort の QoS を選択できる
- ネイティブ依存ゼロ。IL2CPP / AOT 互換のコンパイル時 msg 生成
- ROS 2 の `rcl_context_t` / `rcl_node_t` に対応する `Rcl.Context` / `Rcl.Node` の 2 層 API
- Unity 6000.3 (.NET Standard 2.1) で検証済み。クロスプラットフォームビルドに対応

> [!NOTE]
> 互換性の対象範囲と検証方針は [docs/compatibility.md](docs/compatibility.md)、
> ROS 2 実装との相互運用確認は [docs/interop.md](docs/interop.md) を参照してください。

## クイックスタート

このリポジトリを clone した状態で、別の .NET コンソールアプリから `ROSettaDDS` を参照して試せます。

```sh
dotnet new console -n MyROSettaDDSApp
dotnet add MyROSettaDDSApp/MyROSettaDDSApp.csproj reference src/rosettadds/rosettadds.csproj
```

`Program.cs` を次のように書くと、`std_msgs/msg/String` を publish / subscribe できます。
公開 API は ROS 2 の `rcl_context_t` / `rcl_node_t` と同じく 2 層に分かれています。

- `Rcl.Context` がドメイン共通の DDS 資源 (UDP transport × 4、DiscoveryDb、SPDP / SEDP) を所有します。
- `Rcl.Node` が `Publisher<T>` / `Subscription<T>` / `ServiceClient<TReq,TRes>` を生成するユーザー操作対象です。

```csharp
using ROSettaDDS.Rcl;
using ROSettaDDS.Msgs.Std;

using var context = new Context(new ContextOptions
{
    DomainId = 0,
    EntityName = "rosettadds_demo",
    // 既定で全ローカル NIC を列挙・広告するため、LAN 上のノードから到達できる。
    // loopback に限定したい場合 (ROS_LOCALHOST_ONLY=1 相当) は LocalhostOnly = true を指定。
});
context.Start();

using var node = new Node(context, "rosettadds_demo");

// subscribe
node.CreateSubscription<StringMessage>(
    "chatter", StringMessageSerializer.Instance,
    (msg, source) => Console.WriteLine($"I heard: '{msg.Data}' from {source}"),
    StringMessage.DdsTypeName);

// publish
var pub = node.CreatePublisher<StringMessage>(
    "chatter", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
await pub.PublishAsync(new StringMessage("Hello rosettadds"));
```

> [!NOTE]
> 旧 API である `ROSettaDDS.Dds.DomainParticipant` は後方互換のため残っており、内部で
> `Rcl.Context` + `Rcl.Node` を生成して委譲します。新規コードでは `ROSettaDDS.Rcl` 名前空間を
> 直接使ってください。移行の詳細は [docs/compatibility.md](docs/compatibility.md#legacy-domainparticipant)
> を参照してください。

talker / listener を別プロセスに分けた完成版サンプルは
[`samples/TalkerListener`](samples/TalkerListener) にあります。別シェルで起動してください。

```sh
dotnet run --project samples/TalkerListener -- listener
dotnet run --project samples/TalkerListener -- talker
```

listener 側に `I heard: 'Hello rosettadds: N'` が出れば送受信できています。

## カスタムメッセージを生成する

`.msg` から CDR 互換な C# 型 (`struct` + `ICdrSerializer<T>`) をコンパイル時生成します
(IL2CPP / AOT 互換)。ランタイム生成は行いません。生成方法は 2 つあります。

### Source Generator (.NET プロジェクト向け、生成物のコミット不要)

`.msg` を `AdditionalFiles` に登録すると、ビルド時に透過的に C# 型が生成されます。

```xml
<ItemGroup>
  <ProjectReference Include="path/to/ROSettaDDS.SourceGenerator.csproj"
                    OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
</ItemGroup>

<ItemGroup>
  <AdditionalFiles Include="msgs\**\*.msg" ROSettaDDSMsgPackage="sample_msgs" />
  <CompilerVisibleItemMetadata Include="AdditionalFiles" MetadataName="ROSettaDDSMsgPackage" />
</ItemGroup>
```

例えば `msgs/sample_msgs/msg/Demo.msg` を

```
std_msgs/Header header
string name
float64[] values
int32 count
```

とすると、`ROSettaDDS.Msgs.Sample` 名前空間に `Demo` 型と `DemoSerializer` が生成され、
そのまま使えます。

```csharp
using ROSettaDDS.Msgs.Sample;

var demo = new Demo(header, "custom message", new[] { 1.5, 2.5, 3.5 }, 7);
```

動かせる実例は [`samples/CustomMsgGen`](samples/CustomMsgGen) を参照してください。

### CLI (標準 msg の保守、Unity への生成向け)

`<input>/<package>/msg/<Name>.msg` を走査して `.cs` を出力します。Unity の Assets 配下へ
生成する場合や、生成物をコミットして管理したい場合に使います。

```sh
# 標準 msg を再生成 (入力: msgs/, 出力: src/rosettadds/Msgs/)
dotnet run --project tools/rosettadds-genmsg -- --input msgs --output src/rosettadds/Msgs
```

> [!NOTE]
> 文法サポート範囲・命名ポリシー・各環境の使い方は [docs/msg-codegen.md](docs/msg-codegen.md) を参照してください。

## Topic の診断・周波数監視

`TopicDiagnostics` を使うと、Discovery から収集した Topic 一覧 (`GetTopics`)、
特定 Topic の詳細 (`GetTopicInfo`)、およびメッセージ到達レートの監視 (`CreateFrequencyMonitor`)
が行えます。`Context.Start()` は外部 discovery および DDS 通信を開始するために必要です。
Subscriber のローカルオブジェクト作成自体は `Start()` 前でも可能ですが、外部からの
データ受信や publisher とのマッチには `Start()` が必要です。
`GetTopics` / `GetTopicInfo` はローカル snapshot を返すため `Start()` 前でも呼べます。
`CreateFrequencyMonitor` は内部で subscriber を作成して受信・マッチを行うため
`Start()` が必要です。

```csharp
using ROSettaDDS.Rcl;
using ROSettaDDS.Rcl.Diagnostics;

using var context = new Context(new ContextOptions
{
    DomainId = 0,
    EntityName = "rosettadds_diag",
});
context.Start();

using var node = new Node(context, "rosettadds_diag");

// 全 topic の一覧
using var diag = node.CreateTopicDiagnostics();
var topics = diag.GetTopics();
foreach (var t in topics)
    Console.WriteLine($"{t.TopicName}: pub={t.PublisherCount} sub={t.SubscriberCount}");

// 特定 topic の情報
var info = diag.GetTopicInfo("/chatter");
if (info is not null)
    Console.WriteLine($"type={string.Join(",", info.RosTypeNames)}");

// 周波数監視 (Best Effort reader を内部で作成)
using var monitor = diag.CreateFrequencyMonitor(
    "/chatter",
    new TopicFrequencyOptions { WindowSize = 1000 });

// publisher が接続するのを待つ
if (await monitor.WaitForMatchedAsync(minCount: 1, TimeSpan.FromSeconds(5)))
{
    await Task.Delay(3000); // サンプルを蓄積
    var stats = monitor.GetStatistics();
    Console.WriteLine($"{stats.RateHz:F1} Hz (samples={stats.SampleCount})");
}
```

> [!NOTE]
> `CreateFrequencyMonitor` は内部で **RawSubscription (subscriber)** を作成するため、
> 監視対象 topic の一時的な subscriber 数が 1 増えます。また `TopicDiagnostics` を
> `Dispose` すると配下の全 `TopicFrequencyMonitor` も解放されます。
> `Node` が `TopicDiagnostics` を追跡して自動的に `Dispose` するため、明示的な
> `Dispose` は必須ではなく推奨です。

## QoS を指定する

`Node.CreatePublisher` は既定で Reliable publisher を作ります。ROS 2 の sensor-data 相当の
Best Effort subscriber へ送る場合は、publisher 作成時に QoS を明示します。

```csharp
using ROSettaDDS.Dds.QoS;

using var pub = node.CreatePublisher<StringMessage>(
    "chatter",
    StringMessageSerializer.Instance,
    ReliabilityQos.BestEffort,
    DurabilityQos.Volatile,
    StringMessage.DdsTypeName);
```

## 開発環境・ビルド

Nix flake で ROS 2 Humble + Fast DDS の RMW (`rmw_fastrtps_cpp`) + .NET 8 SDK が揃います。

```sh
# Nix flake の devShell に入る (direnv 利用時は自動 reload)
nix develop
```

devShell では `RMW_IMPLEMENTATION=rmw_fastrtps_cpp` を既定値にしています。
`ros.cachix.org` を利用するには `trusted-users` もしくは `trusted-substituters` に追加してください。
未設定だと ROS パッケージを自前ビルドすることになります。

```sh
dotnet build
dotnet test
```

## ドキュメント

| ドキュメント | 内容 |
| --- | --- |
| [docs/compatibility.md](docs/compatibility.md) | 互換性の対象範囲と検証方針 |
| [docs/interop.md](docs/interop.md) | ROS 2 実装との相互運用確認 |
| [docs/msg-codegen.md](docs/msg-codegen.md) | msg コード生成 (rosidl 相当) の文法・命名ポリシー |

サンプルは [`samples/`](samples) 配下にあります (`TalkerListener` / `CustomMsgGen` / `SpdpDemo`)。
