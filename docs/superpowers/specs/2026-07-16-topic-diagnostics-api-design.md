# Topic Diagnostics API Design

## Goal

ROSettaDDS に ROS 2 の `ros2 topic list`、`topic info`、`topic hz` に相当する診断機能を、CLI ではなく再利用可能な C# API として追加する。
主な利用者は .NET アプリケーションと Unity であり、ROS 2 CLI の文字列出力互換は対象外とする。

## Scope

`ROSettaDDS.Rcl.Diagnostics` 名前空間に `TopicDiagnostics` を追加し、`Node.CreateTopicDiagnostics()` から取得できるようにする。

```csharp
using var diagnostics = node.CreateTopicDiagnostics();
var topics = diagnostics.GetTopics();
var info = diagnostics.GetTopicInfo("/chatter");
using var monitor = diagnostics.CreateFrequencyMonitor("/chatter");
```

公開 API の初版署名は次のとおりとする。

```csharp
public TopicDiagnostics CreateTopicDiagnostics();
public IReadOnlyList<TopicInfo> GetTopics();
public TopicInfo? GetTopicInfo(string topicName);
public TopicFrequencyMonitor CreateFrequencyMonitor(
    string topicName, TopicFrequencyOptions? options = null);

public sealed class TopicFrequencyOptions
{
    public ReliabilityQos Reliability { get; init; } = ReliabilityQos.BestEffort;
    public DurabilityQos Durability { get; init; } = DurabilityQos.Volatile;
    public int WindowSize { get; init; } = 10_000;
}

public Task<bool> WaitForMatchedAsync(
    int minCount, TimeSpan timeout, CancellationToken cancellationToken = default);
public TopicFrequencyStatistics GetStatistics();
```

`WindowSize` は 2 以上、既定値以下の実装上限を設ける。無効値は
`ArgumentOutOfRangeException`、未発見 topic は `TopicNotFoundException`、複数型 topic は
`AmbiguousTopicTypeException` とする。

### Topic listing and info

- `GetTopics()` は呼び出し時点のスナップショットを返す。
- 同じ `Context` 内の全 Node の local endpoint と SEDP で発見した remote endpoint を GUID で集約する。
- 通常の `rt/` topic のみを対象とし、`rq/` と `rr/` の service 内部 topic は除外する。
- topic 名は `/chatter` 形式、表示用型名は `std_msgs/msg/String` 形式へ demangle する。
- `TopicInfo` は topic 名、型名一覧、publisher/subscriber 数、endpoint 詳細を持つ。
- endpoint 詳細は GUID、種別、local/remote、型、Reliability、Durability など SEDP から取得可能な QoS を含む。
- node 名・namespace は現在の discovery metadata に存在しないため、推測して追加しない。
- 複数型が存在する topic は型名一覧にすべて残す。

`Context` に graph registry を設け、全 local endpoint metadata と remote endpoint metadata を
Context 単位の lock 下で値コピーする内部 `CreateGraphSnapshot()` を提供する。remote 側も
`DiscoveryDb` に同じ lock 下の `CreateEndpointSnapshot()` を追加する。公開 DTO は immutable な
値型/読み取り専用コレクションとし、取得後に内部状態が変化しないことを保証する。集合全体の
snapshot はこの graph lock の単一読み取り区間で作成する。返却順は topic 名、endpoint GUID の
ordinal 順とし、同一 GUID は一度だけ含める。

endpoint は表示用の `RosTypeName` と wire matching 用の `DdsTypeName` を別々に保持する。
DDS 型名が空の場合は `RosTypeName` を生成せず「型未発見」と扱う。ROS 形式でない DDS 型名は
demangle せずそのまま表示する。`TopicNameMangler.DemangleTopic` の結果には diagnostics 側で
先頭 `/` を付加し、`rt/foo/bar` を `/foo/bar` とする。

### Frequency monitoring

`CreateFrequencyMonitor` は対象 topic の wire DDS 型名を確認し、型をデシリアライズせず raw CDR
payload の到着時刻だけを計測する一時 reader を作成する。

- 型未発見、または複数型の場合は暗黙に選択せず失敗する。
- `ParticipantEndpointFactory.CreateRawReader` と内部 `RawSubscription` を追加し、serializer
  なしで `IUserReader.PayloadReceived` を利用する。
- raw reader は既存の Node の endpoint registry、receiver 登録、SEDP 広告、remote writer match
  の経路を使う。callback では payload を保持せず `Stopwatch.GetTimestamp()` のみを記録する。
- 既定 QoS は Best Effort/Volatile とし、Reliable writer と Best Effort writer の双方を測定対象に
  できる。options で Reliable/Volatile などを明示指定でき、非互換 endpoint は matched 数に含めない。
- SEDP に広告する型名は demangle 前の正確な `DdsTypeName` とする。
- 固定サイズのリングバッファで直近 `WindowSize` 件を保持する。
- `TopicFrequencyStatistics` は `RateHz`、`MinInterval`、`MaxInterval`、`MeanInterval`、`StandardDeviation`、`SampleCount`、`WindowDuration`、`HasData` を返す。
- `SampleCount` は保持している timestamp 数とする。2件以上かつ最後の timestamp が最初より後の場合だけ `HasData=true` とする。
- interval は隣接 timestamp の差分で、`RateHz = (SampleCount - 1) / WindowDuration.TotalSeconds` とする。min/max/mean/stddev も interval を対象とし、stddev は母標準偏差とする。
- interval と duration は `TimeSpan`、rate は `double` とする。同一 timestamp のみ、または interval がない場合は `HasData=false` とする。ring buffer 上書き後は保持中 timestamp だけで再計算する。
- `WaitForMatchedAsync` は既存 API と同じ引数で提供し、timeout は `false`、cancellation は `OperationCanceledException` とする。
- `Dispose` で reader、receiver 登録、SEDP 広告を解除する。
- 計測中は対象 topic の subscriber 数が一時的に 1 増えることを API ドキュメントに明記する。
- 初版では echo、payload サイズ、帯域幅、delay は対象外とする。

`TopicDiagnostics` は作成した monitor を追跡する。`TopicDiagnostics.Dispose` は全 monitor を
dispose し、`Node.Dispose` は diagnostics を先に停止してから endpoint を解除する。`Context.Dispose`
からの Node dispose も同じ順序とする。monitor の Dispose は待機中の match waiter を linked
cancellation token で解除し、callback と同時実行されても統計バッファを破壊しない。二重 Dispose は
no-op、Dispose 後の操作は `ObjectDisposedException` とする。

## Errors and lifecycle

- `GetTopicInfo` は未発見 topic に対して `null` を返す。
- monitor 作成時の未発見 topic、複数型、無効な引数は明示的な例外で通知する。
- `Context` または `Node` の破棄後の操作は既存 API と同様に `ObjectDisposedException` とする。
- snapshot は graph lock の読み取り区間で endpoint の追加・削除、remote endpoint の発見・消失と
 競合しない一貫した値コピーを返す。
- 同一 timestamp や短い計測窓を安全に扱い、負の間隔とゼロ除算を発生させない。

## Testing

既存の `tests/rosettadds.Tests` に以下を追加する。

- local/remote endpoint の topic 集約
- service topic の除外
- topic/type name の demangle
- endpoint の GUID、local/remote、QoS
- リングバッファと rate、min/max、標準偏差、空状態
- monitor の開始、停止、Dispose 後の endpoint 解除
- loopback payload の受信間隔計測
- Best Effort、Reliable、QoS 混在 publisher
- 空型名、複数 DDS 型、非 ROS 型名
- `WindowSize` の境界、同一 timestamp、ring wrap-around、極端な interval
- Node/Context 先行 Dispose、callback と snapshot/Dispose の競合
- SEDP 広告前の monitor Dispose、wait timeout/cancellation
- remote endpoint の update/lost と snapshot の同時実行

`dotnet build` では `netstandard2.1` target を明示的に検証し、Unity EditMode または Player/IL2CPP
検証では monitor の作成、受信、統計取得、Dispose を確認する。

既存の ROS 2 Fast DDS interop 検証には、ROS 2 topic の発見と frequency monitor の計測ケースを追加する。

## Alternatives considered

1. `TopicDiagnostics` を `Node` から生成する。graph 参照と一時 reader の所有を分離し、`Node` を薄く保てるため採用する。
2. `Node` に診断メソッドを直接追加する。呼び出しは簡単だが、Node に診断責務が混在する。
3. raw graph と raw subscription だけを公開する。実装は小さいが、利用者が集約・demangle・統計を再実装することになる。
