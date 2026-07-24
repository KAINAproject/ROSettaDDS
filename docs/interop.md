# ROS 2 interop 検証

この文書は rosettadds と ROS 2 node の相互運用を確認するための手順を定義する。
unit tests は wire format や loopback の正しさを確認し、interop scripts は実際の ROS 2 RMW 実装と通信できることを確認する。

## 前提

- ROS 2 Humble が利用できること
- `demo_nodes_cpp` が利用できること
- `dotnet` SDK 8.0 が利用できること
- 初期対象 RMW は `rmw_fastrtps_cpp`

Nix devShell を使う場合:

```sh
nix develop
```

## Fast DDS talker/listener

Fast DDS baseline は次のスクリプトで確認する。

```sh
scripts/interop/fastdds_talker_listener.sh
```

Nix devShell では `ros2` CLI の Python extension が macOS の動的ライブラリ解決で失敗する場合があるため、
スクリプトは `$AMENT_PREFIX_PATH/lib/demo_nodes_cpp/{talker,listener}` が存在すれば `ros2 run` を経由せず直接実行する。

このスクリプトは以下を確認する。

- rosettadds publisher から ROS 2 `demo_nodes_cpp listener` に `std_msgs/msg/String` が届くこと
- ROS 2 `demo_nodes_cpp talker` から rosettadds subscriber に `std_msgs/msg/String` が届くこと
- `ROS_LOCALHOST_ONLY=1` とスクリプトが選んだ `ROS_DOMAIN_ID` で loopback 通信できること
- 検証前に `ros2 daemon stop` して古い graph cache の影響を避けること

## Fast DDS large payload

Fast DDS が `DATA_FRAG` を使うサイズの payload は次のスクリプトで確認する。

```sh
scripts/interop/fastdds_large_string.sh
```

このスクリプトは ROS 2 CLI から 32KB 超の `std_msgs/msg/String` を publish し、rosettadds listener が実 payload を受信できることを確認する。
`ros2 topic pub` を使うため、`ros2` CLI が動作する Linux または ROS 2 セットアップ済み環境で実行する。

## 判定基準

interop の成功条件は introspection 結果ではなく、実際の受信ログにする。

- ROS 2 listener 側で `Hello rosettadds` を受信している
- rosettadds listener 側で `Hello World` または `Hello rosettadds` を受信している
- large payload 検証では rosettadds listener 側で `large-payload-` を含む message を受信している

## Vendor ID 切替の検証

`VendorId.ROSettaDDS` を eProsima 借用 (`0x010F`) から独自値 (`0x013F`) へ切替えた後も、
Fast DDS (`rmw_fastrtps_cpp`) と双方向で疎通することを確認する。
ローカル検証では `demo_nodes_cpp` の talker/listener バイナリと `samples/TalkerListener` を
`ROS_LOCALHOST_ONLY=1` / 同一 `ROS_DOMAIN_ID` で対向させ、両方向で実 message が届くことを確認した。

## サービスクライアントの相互運用確認 (例: example_interfaces/AddTwoInts)

ROS 2 側でサービスサーバを起動する:

```sh
ros2 run examples_rclpy_minimal_service service
# または C++ 版
ros2 run examples_rclcpp_minimal_service service_main
```

`example_interfaces/srv/AddTwoInts.srv` (`int64 a` / `int64 b` `---` `int64 sum`) を
ROSettaDDS 側で生成 (Source Generator または rosettadds-genmsg) し、クライアントから呼び出す:

```csharp
using var context = new ROSettaDDS.Rcl.Context(new ROSettaDDS.Rcl.ContextOptions
{
    DomainId = 0,
    EntityName = "rosettadds_svc",
});
context.Start();

using var node = new ROSettaDDS.Rcl.Node(context, "rosettadds_svc");

using var client = node.CreateServiceClient(AddTwoIntsService.Descriptor, "add_two_ints");
if (!await client.WaitForServiceAsync(TimeSpan.FromSeconds(5)))
{
    Console.Error.WriteLine("service not available");
    return;
}

var resp = await client.CallAsync(new AddTwoIntsRequest(2, 3), TimeSpan.FromSeconds(3));
Console.WriteLine($"2 + 3 = {resp.Sum}");
```

`2 + 3 = 5` が表示されれば、request/reply の相関 (Fast DDS の related_sample_identity) を含めて
相互運用できている。

## Unity ROS 2 performance tests

疎通確認は `demo_nodes_cpp` や ROS 2 CLI で実 message の到達を確認する。
性能計測は別系統として、`tools/ros2-perf-helper` と
`tools/rosettadds-perf-runner` を使う。

性能計測では ROS 2 devShell 内で起動した runner が、起動済み Unity Editor に
`uloop execute-dynamic-code` で Player build を依頼し、その後 C++ helper process と
Unity Standalone Player を起動して JSON Lines と sentinel file で同期する。
runner は Unity Editor executable を shell から起動しない。Unity Editor / Unity Player は
ROS 2 CLI や ROS 2 環境を持たず、DDS/RTPS 通信だけを行う。
対象は Humble + Fast DDS (`rmw_fastrtps_cpp`) の同一マシン loopback 通信で、
マシン間通信や Cyclone DDS は初期対象外。

### Linux での helper 動作確認

`tools/ros2-perf-helper` の C++ helper は、Unity Editor が無い環境でも単体で
build / 動作確認できる。Nix devShell (`flake.nix` の `rosEnv`) 配下で次の 2
スクリプトを使う。

```sh
nix develop
scripts/ros2/build_helper.sh      # colcon build
scripts/ros2/verify_helper.sh      # 6 ケースの smoke
```

`scripts/ros2/verify_helper.sh` は次の 6 ケースを連続実行する。すべて
helper の stdout を JSON Lines で受け取り、`done.received` または `error.message`
で合否判定する。

1. reliable pub<->sub, 1000 messages, 64 B
2. best\_effort pub<->sub, 500 messages, 128 B
3. fanout: 1 pub vs 4 subs, 250 messages, 32 B
4. idle timeout: pub 10 / sub 1000 expected / idle 2s
5. invalid `--mode` の JSON error event
6. large payload 100 messages × 32 KiB

ヘルパーが `demo_nodes_cpp` の talker / listener と直接通信することも
確認済み (helper sub <-> talker / helper pub <-> listener) で、JSON Lines
に `"received":<件数>` または listener の `I heard: [...]` ログで疎通を
確認できる。

### Unity Player Profiler 計測

```sh
nix develop
scripts/ros2/build_helper.sh
uloop execute-dynamic-code --project-path Ros2Unity --code 'return "ok";'
dotnet run --project tools/rosettadds-perf-runner -- \
  --scenario unity-to-ros2-reliable-1024 \
  --capture-frames 1200
```

生成物は `artifacts/perf/<run-id>/` に保存される。主な内容は
`manifest.json`、scenario ごとの `metrics.ndjson`、`player.profiler.raw`、
Player log、helper stdout/stderr log。いずれかの scenario が失敗した場合 runner は
非 0 で終了するが、成功/失敗の内訳と生成済み artifact は `manifest.json` に残る。

## FrequencyMonitor の相互運用確認

ROS 2 (Fast DDS) publisher に対して rosettadds の `TopicFrequencyMonitor` で
メッセージレートを計測できることを確認する。検証は ROS 2 CLI (`ros2 topic pub`) が
使える環境で実行し、初回 publish 前に discovery を待つため最初の数秒の欠損は許容する。

### 検証用コード

以下のコード片をコンソールアプリケーションとして実行する。
(ROSettaDDS への参照が必要。詳細は README.ja.md の「クイックスタート」を参照。)

```csharp
using ROSettaDDS.Dds.QoS;
using ROSettaDDS.Rcl;
using ROSettaDDS.Rcl.Diagnostics;

var options = new ContextOptions
{
    DomainId = 0,
    EntityName = "fm_interop",
    LocalhostOnly = true,
};
using var context = new Context(options);
using var node = new Node(context, "fm_interop");
context.Start();

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\nShutting down...");
};

using var diag = node.CreateTopicDiagnostics();

try
{

// 事前に discovery を待つ (ROS 2 publisher が起動済みであること)
await Task.Delay(3000, cts.Token);

// ===== BestEffort sensor-data (10 Hz) =====
using var beMonitor = diag.CreateFrequencyMonitor("/sensor_data");
if (!await beMonitor.WaitForMatchedAsync(1, TimeSpan.FromSeconds(5), cts.Token))
{
    Console.Error.WriteLine("BE: no matched publisher");
    return 1;
}
await Task.Delay(3000, cts.Token);
var beStats = beMonitor.GetStatistics();
Console.WriteLine($"BE: rate={beStats.RateHz:F1} Hz samples={beStats.SampleCount}");
bool beOk = beStats.HasData && beStats.RateHz is >= 8 and <= 12;

// ===== Reliable (100 Hz) =====
using var relMonitor = diag.CreateFrequencyMonitor(
    "/reliable_data",
    new TopicFrequencyOptions { Reliability = ReliabilityQos.Reliable });
if (!await relMonitor.WaitForMatchedAsync(1, TimeSpan.FromSeconds(5), cts.Token))
{
    Console.Error.WriteLine("Rel: no matched publisher");
    return 1;
}
await Task.Delay(5000, cts.Token);
var relStats = relMonitor.GetStatistics();
Console.WriteLine($"Rel: rate={relStats.RateHz:F1} Hz samples={relStats.SampleCount}");
bool relOk = relStats.HasData && relStats.RateHz is >= 80 and <= 120;

Console.WriteLine($"BE: {(beOk ? "PASS" : "FAIL")}  Rel: {(relOk ? "PASS" : "FAIL")}");
return beOk && relOk ? 0 : 1;

}
catch (OperationCanceledException)
{
    Console.WriteLine("Cancelled by user");
    return 1;
}
```

### 検証手順

```sh
# シェル 1: BestEffort sensor-data 10 Hz
ros2 topic pub /sensor_data std_msgs/msg/String "data: 's-{01}'" \
  --qos-reliability best_effort --qos-durability volatile --rate 10 --max-messages 100

# シェル 2: Reliable 100 Hz
ros2 topic pub /reliable_data std_msgs/msg/String "data: 'f-{01}'" \
  --qos-reliability reliable --qos-durability volatile --rate 100 --max-messages 1000

# シェル 3: 上記コードを実行
dotnet run --project <path-to-test-app>
```

### Matched 判定の検証

`WaitForMatchedAsync` で publisher の接続を検出できることを確認する。
`CreateFrequencyMonitor` は topic が discovery で見つかっている必要があるため、
先に publisher を発見させてから monitor を作成する。

```csharp
using ROSettaDDS.Msgs.Std;

using var diag = node.CreateTopicDiagnostics();

// ローカル Publisher を作成し topic を discovery に登録
using var pub = node.CreatePublisher<StringMessage>(
    "/lazy_pub", StringMessageSerializer.Instance, StringMessage.DdsTypeName);
await Task.Delay(500, cts.Token); // SEDP 広告の伝搬を待つ

using var monitor = diag.CreateFrequencyMonitor("/lazy_pub");

// Publisher が存在するので WaitForMatchedAsync は true を返す
bool matched = await monitor.WaitForMatchedAsync(1, TimeSpan.FromSeconds(5), cts.Token);
Console.WriteLine($"matched: {matched}"); // true

pub.Dispose();
await Task.Delay(500, cts.Token);

// Publisher が消えたので WaitForMatchedAsync は false (timeout)
matched = await monitor.WaitForMatchedAsync(1, TimeSpan.FromSeconds(2), cts.Token);
Console.WriteLine($"matched after dispose: {matched}"); // false
```

### 判定基準

- BestEffort / Reliable ともに `GetStatistics().HasData == true` かつ `RateHz > 0` を満たすこと
- レート値は ±20% の許容範囲内であること
- `WaitForMatchedAsync` は publisher 生存中に `true` を返すこと (不在時は `false` または timeout)
- 同一 topic に一時 subscriber が追加されることを許容すること

## 次に追加する検証

- Best Effort publisher/subscriber の組み合わせ
- Cyclone DDS (`rmw_cyclonedds_cpp`) ※本環境では未提供のため未検証
- `std_msgs/Header` など string 以外の msg
- discovery unregister の反映
- Linux CI または self-hosted runner での定期実行
