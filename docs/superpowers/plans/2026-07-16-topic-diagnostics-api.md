# Topic Diagnostics API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ROSettaDDS に topic の一覧・詳細・受信周波数を取得する再利用可能な C# 診断 API を追加する。

**Architecture:** `Node.CreateTopicDiagnostics()` が `TopicDiagnostics` を生成する。Context 単位の graph registry が local/remote endpoint の immutable snapshot を作り、`TopicDiagnostics` はそれを ROS 名・型名へ変換する。周波数監視は serializer 不要の `RawSubscription` を既存の endpoint 登録・SEDP・match 経路へ接続し、固定長 timestamp buffer から統計を計算する。

**Tech Stack:** C# / .NET 8 / netstandard2.1、既存の RTPS receiver、SEDP discovery、xUnit、FluentAssertions、Unity 6000.3 / IL2CPP。

---

## File Map

- Create: `src/rosettadds/Rcl/Diagnostics/TopicDiagnostics.cs` - graph 集約、public API、monitor 所有
- Create: `src/rosettadds/Rcl/Diagnostics/TopicModels.cs` - immutable topic/endpoint DTO
- Create: `src/rosettadds/Rcl/Diagnostics/TopicFrequencyMonitor.cs` - raw reader lifecycle と timestamp 集計
- Create: `src/rosettadds/Rcl/Diagnostics/TopicFrequencyStatistics.cs` - 統計値 DTO
- Create: `src/rosettadds/Rcl/Diagnostics/TopicDiagnosticsExceptions.cs` - 未発見/複数型例外
- Create: `src/rosettadds/Dds/RawSubscription.cs` - serializer 不要の raw callback wrapper
- Modify: `src/rosettadds/Dds/ParticipantEndpointFactory.cs` - raw reader factory
- Modify: `src/rosettadds/Dds/UserEndpointManager.cs` - raw reader の登録・解除・metadata snapshot
- Modify: `src/rosettadds/Dds/EndpointRegistry.cs` - local endpoint metadata snapshot
- Modify: `src/rosettadds/Rcl/Context.cs` - Context graph snapshot と diagnostics tracking
- Modify: `src/rosettadds/Rcl/Node.cs` - diagnostics生成、raw endpoint経路、dispose順序
- Modify: `src/rosettadds/Discovery/DiscoveryDb.cs` - writer/reader同時値コピー snapshot
- Modify: `src/rosettadds/Rcl/Naming/TopicNameMangler.cs` - diagnostics用ROS topic名規約のテストまたは補助API
- Create: `tests/rosettadds.Tests/Rcl/Diagnostics/TopicDiagnosticsTests.cs` - graph集約・DTO・型名
- Create: `tests/rosettadds.Tests/Rcl/Diagnostics/TopicFrequencyMonitorTests.cs` - 統計・境界・lifecycle
- Modify: `README.ja.md` - 公開 API の使用例
- Modify: `docs/interop.md` - ROS 2 Fast DDS の diagnostics 検証手順

### Task 1: Graph snapshot の内部基盤

**Files:**
- Modify: `src/rosettadds/Discovery/DiscoveryDb.cs`
- Modify: `src/rosettadds/Dds/EndpointRegistry.cs`
- Modify: `src/rosettadds/Dds/UserEndpointManager.cs`
- Modify: `src/rosettadds/Rcl/Context.cs`
- Test: `tests/rosettadds.Tests/Discovery/DiscoveryDbTests.cs`
- Test: `tests/rosettadds.Tests/Rcl/Diagnostics/TopicDiagnosticsTests.cs`

- [ ] **Step 1: Write failing snapshot tests**

  local endpoint metadata と remote endpoint metadata が同一 snapshot に値コピーされ、取得後の `DiscoveredEndpointData` 更新で結果が変化しないテストを書く。writer/reader の片方だけが追加・削除される競合では、snapshot が追加前または削除後のどちらか一貫した集合になることを検証する。

- [ ] **Step 2: Run tests to verify failure**

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --filter FullyQualifiedName~TopicDiagnostics --no-restore`
  Expected: snapshot API が未定義、または immutable snapshot assertion が FAIL。

- [ ] **Step 3: Implement internal graph snapshot**

  `DiscoveryDb` に writer/reader metadata を同じ lock 区間で immutable value copy 化するメソッドを追加する。`EndpointRegistry` は local `DiscoveredEndpointData` のコピーを返し、`UserEndpointManager` は local snapshot を公開する。`Context` は全 Node の local snapshot と `DiscoveryDb` の remote snapshot を Context graph lock 下でまとめ、GUID 重複を除外する。snapshot の返却順は topic 名、endpoint GUID の ordinal 順に固定する。

- [ ] **Step 4: Run focused tests**

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --filter FullyQualifiedName~TopicDiagnostics --no-restore`
  Expected: PASS。

- [ ] **Step 5: Commit**

  ```sh
  git add src/rosettadds/Discovery/DiscoveryDb.cs src/rosettadds/Dds/EndpointRegistry.cs src/rosettadds/Dds/UserEndpointManager.cs src/rosettadds/Rcl/Context.cs tests/rosettadds.Tests
  git commit -m "feat: graph snapshotの内部基盤を追加"
  ```

### Task 2: Topic DTO と一覧・詳細 API

**Files:**
- Create: `src/rosettadds/Rcl/Diagnostics/TopicModels.cs`
- Create: `src/rosettadds/Rcl/Diagnostics/TopicDiagnosticsExceptions.cs`
- Create: `src/rosettadds/Rcl/Diagnostics/TopicDiagnostics.cs`
- Modify: `src/rosettadds/Rcl/Node.cs`
- Modify: `src/rosettadds/Rcl/Naming/TopicNameMangler.cs`
- Modify: `src/rosettadds/Rcl/Naming/TypeNameMangler.cs`
- Test: `tests/rosettadds.Tests/Rcl/Diagnostics/TopicDiagnosticsTests.cs`

- [ ] **Step 1: Write failing API tests**

  `/chatter` の topic 集約、`rt/` 以外の除外、`std_msgs::msg::dds_::String_` から `std_msgs/msg/String` への表示変換、GUID/QoS/local-remote DTO、複数型保持、未発見時の `null` をテストする。返却 DTO は取得後の内部更新で変化しないことも検証する。

- [ ] **Step 2: Run tests to verify failure**

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --filter FullyQualifiedName~TopicDiagnostics --no-restore`
  Expected: `TopicDiagnostics` と DTO が未定義のため FAIL。

- [ ] **Step 3: Implement immutable DTO and API**

  `TopicEndpointInfo` は GUID、kind、local/remote、topic、`DdsTypeName`、`RosTypeName`、Reliability、Durability を保持する immutable 型にする。`TopicInfo` は topic 名、distinct type list、publisher/subscriber count、readonly endpoint list を保持する。`TopicDiagnostics.GetTopics()` は `rt/` のみを集約し、topic 名は diagnostics 側で先頭 `/` を付ける。`Node.CreateTopicDiagnostics()` は Node の Context と local endpoint source を渡す。

- [ ] **Step 4: Validate edge cases**

  `rt/foo/bar`、`rt/`、既に `/` を含む入力、非 ROS 型名、空 DDS 型名、service topic のテストを追加し、`ArgumentException` と `null` の境界を固定する。

- [ ] **Step 5: Run focused tests and commit**

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --filter FullyQualifiedName~TopicDiagnostics --no-restore`
  Expected: PASS。

  ```sh
  git add src/rosettadds/Rcl/Diagnostics src/rosettadds/Rcl/Node.cs src/rosettadds/Rcl/Naming tests/rosettadds.Tests/Rcl/Diagnostics
  git commit -m "feat: topic一覧と詳細診断APIを追加"
  ```

### Task 3: Raw reader と subscription lifecycle

**Files:**
- Create: `src/rosettadds/Dds/RawSubscription.cs`
- Modify: `src/rosettadds/Dds/ParticipantEndpointFactory.cs`
- Modify: `src/rosettadds/Dds/UserEndpointManager.cs`
- Modify: `src/rosettadds/Rcl/Node.cs`
- Test: `tests/rosettadds.Tests/Rcl/Diagnostics/TopicFrequencyMonitorTests.cs`

- [ ] **Step 1: Write failing raw reader tests**

  serializer なしで raw payload callback を受け取れること、default Best Effort/Volatile が Reliable と Best Effort writer の双方に match できること、receiver登録・SEDP広告・Dispose解除を検証する。

- [ ] **Step 2: Run tests to verify failure**

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --filter FullyQualifiedName~TopicFrequencyMonitor --no-restore`
  Expected: raw factory/wrapper が未定義のため FAIL。

- [ ] **Step 3: Implement raw endpoint path**

  `ParticipantEndpointFactory.CreateRawReader(ddsTopic, ddsTypeName, reliability, durability)` を追加する。`RawSubscription` は `IUserReader.PayloadReceived` に接続し、payload を保持せず callback を呼ぶ。`UserEndpointManager` と `Node` は既存 typed subscription と同じ順序で register、match、advertiseし、Dispose時は callback解除、receiver解除、endpoint unregister、SEDP unregister の順に処理する。

- [ ] **Step 4: Test lifecycle races**

  SEDP広告前のDispose、二重Dispose、callback同時実行、Node先行Dispose、Context先行Disposeをテストする。waiter cancellation用 linked token が所有者Disposeで解除されることを確認する。

- [ ] **Step 5: Run tests and commit**

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --filter FullyQualifiedName~TopicFrequencyMonitor --no-restore`
  Expected: PASS。

  ```sh
  git add src/rosettadds/Dds src/rosettadds/Rcl/Node.cs tests/rosettadds.Tests/Rcl/Diagnostics
  git commit -m "feat: raw topic readerのライフサイクルを追加"
  ```

### Task 4: Frequency monitor と統計

**Files:**
- Create: `src/rosettadds/Rcl/Diagnostics/TopicFrequencyStatistics.cs`
- Create: `src/rosettadds/Rcl/Diagnostics/TopicFrequencyMonitor.cs`
- Modify: `src/rosettadds/Rcl/Diagnostics/TopicDiagnostics.cs`
- Test: `tests/rosettadds.Tests/Rcl/Diagnostics/TopicFrequencyMonitorTests.cs`

- [ ] **Step 1: Write failing deterministic statistics tests**

  injectable clock を使い、timestamp 2件以上で `RateHz = (N-1)/duration`、隣接 interval の min/max/mean、母標準偏差、ring wrap-around、同一 timestamp、空状態を検証する。`SampleCount` は timestamp 数で、2件未満または duration 0 では `HasData=false` とする。

- [ ] **Step 2: Run tests to verify failure**

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --filter FullyQualifiedName~TopicFrequencyMonitor --no-restore`
  Expected: monitor/statistics が未定義のため FAIL。

- [ ] **Step 3: Implement fixed-window monitor**

  `TopicFrequencyOptions` の default は Best Effort/Volatile/10,000、WindowSize は 2 以上かつ上限以下とする。`TopicFrequencyMonitor` は raw callback で `Stopwatch.GetTimestamp()` をリングバッファへ追加し、snapshot時に隣接差分を `TimeSpan` へ変換して統計を計算する。`WaitForMatchedAsync(int, TimeSpan, CancellationToken)` は timeout を false、cancel を `OperationCanceledException` とする。

- [ ] **Step 4: Implement monitor selection errors**

  未発見 topic は `TopicNotFoundException`、空/複数 DDS 型は `AmbiguousTopicTypeException`、無効 options は `ArgumentOutOfRangeException` とする。既知の wire `DdsTypeName` を raw reader の SEDP metadata にそのまま渡す。

- [ ] **Step 5: Run tests and commit**

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --filter FullyQualifiedName~TopicFrequencyMonitor --no-restore`
  Expected: PASS。

  ```sh
  git add src/rosettadds/Rcl/Diagnostics tests/rosettadds.Tests/Rcl/Diagnostics
  git commit -m "feat: topic周波数モニターを追加"
  ```

### Task 5: Integration, documentation, and verification

**Files:**
- Modify: `README.ja.md`
- Modify: `docs/interop.md`
- Modify: `.github/workflows/ci.yml` only if the existing matrix does not explicitly compile netstandard2.1
- Test: `tests/rosettadds.Tests/Integration/*`
- Unity verification: `Ros2Unity` existing diagnostics/playmode test area

- [ ] **Step 1: Add public API documentation**

  README に `GetTopics`、`GetTopicInfo`、`CreateFrequencyMonitor` の最小例と、一時 subscriber により graph 上の subscriber 数が増える注意を記載する。

- [ ] **Step 2: Add ROS 2 interop scenario**

  `docs/interop.md` に Fast DDS publisher と ROSettaDDS monitor の組み合わせ、Best Effort sensor-data と Reliable publisher の検証、matched待機、統計取得、monitor終了を追記する。判定は graph list ではなく実受信件数と rate 出力で行う。

- [ ] **Step 3: Run full verification**

  Run: `dotnet build rosettadds.sln --configuration Release`
  Expected: PASS for net8.0 projects and the `rosettadds` netstandard2.1 target.

  Run: `dotnet test tests/rosettadds.Tests/rosettadds.Tests.csproj --configuration Release --no-build --verbosity normal`
  Expected: all tests PASS.

  Run: `dotnet pack src/rosettadds/rosettadds.csproj --configuration Release --no-build --output artifacts/packages`
  Expected: NuGet package is produced.

  Run: `.github/scripts/check_unity_meta.sh`
  Expected: no missing or orphan `.meta` files under `src/rosettadds`; add `.meta` files for every new Unity-imported file.

- [ ] **Step 4: Run Unity verification**

  Run the repository’s existing Unity EditMode/PlayMode verification scripts and add a monitor lifecycle case to the existing test assembly rather than introducing a separate Unity package. Expected: monitor can create, receive, calculate statistics, and dispose under Unity’s compiler/runtime.

- [ ] **Step 5: Commit documentation and verification changes**

  ```sh
  git add README.ja.md docs/interop.md .github/workflows/ci.yml tests Ros2Unity src/rosettadds
  git commit -m "docs: topic診断APIの利用例とinterop検証を追加"
  ```

## Final Review Checklist

- [ ] `GetTopics` returns immutable, sorted local+remote snapshots and excludes service topics.
- [ ] `GetTopicInfo` preserves both DDS and ROS type names and reports endpoint QoS.
- [ ] Raw monitor defaults to Best Effort/Volatile and uses exact wire DDS type names.
- [ ] Frequency statistics use timestamp count, adjacent intervals, and population standard deviation as specified.
- [ ] Monitor, diagnostics, Node, and Context disposal are idempotent and cancel pending waits.
- [ ] net8.0, netstandard2.1, unit, package, and Unity verification pass.
