using ROSettaDDS.Common.Logging;

namespace ROSettaDDS.Rtps.Writer;

/// <summary>
/// StatefulWriter の非同期タスク追跡と終了待機を担当する内部クラス。
/// Dispose 後に新規タスク登録を拒否する registration gate を持つ。
/// </summary>
internal sealed class BackgroundOperationTracker : IDisposable
{
    private readonly object _lock = new();
    private readonly HashSet<Task> _tasks = new();
    private readonly ILogger _logger;
    private bool _disposed;

    public BackgroundOperationTracker(ILogger? logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// タスクを登録して実行する。Dispose 後は無視される。
    /// </summary>
    public void Run(
        Func<CancellationToken, Task> operation,
        string operationName,
        CancellationToken cancellationToken)
    {
        Task? task;
        lock (_lock)
        {
            if (_disposed) return;
            task = RunAsync(operation, operationName, cancellationToken);
            _tasks.Add(task);
        }

        var captured = task;
        _ = captured.ContinueWith(
            completed =>
            {
                lock (_lock)
                {
                    _tasks.Remove(completed);
                }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// 全タスクの完了をタイムアウト付きで待機する (Stop 用)。
    /// </summary>
    public void WaitForCompletion(TimeSpan timeout)
    {
        Task[] tasks;
        lock (_lock)
        {
            tasks = _tasks.ToArray();
        }

        if (tasks.Length == 0) return;

        try
        {
            if (!Task.WaitAll(tasks, timeout))
            {
                _logger.Warn("BackgroundOperationTracker tasks did not exit cleanly");
            }
        }
        catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is OperationCanceledException))
        {
        }
        catch (Exception ex)
        {
            _logger.Warn("BackgroundOperationTracker tasks did not exit cleanly", ex);
        }
    }

    /// <summary>
    /// 全タスクの完了を無限待機する (Dispose 用)。
    /// </summary>
    public void WaitForCompletion()
    {
        Task[] tasks;
        lock (_lock)
        {
            tasks = _tasks.ToArray();
        }

        if (tasks.Length == 0) return;

        try
        {
            Task.WaitAll(tasks);
        }
        catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is OperationCanceledException))
        {
        }
        catch (Exception ex)
        {
            _logger.Warn("BackgroundOperationTracker tasks did not exit cleanly", ex);
        }
    }

    /// <summary>
    /// Registration gate を閉じ、全タスクの完了を待ってから返る。
    /// Dispose 後に Run されても無視される。
    /// </summary>
    public void Dispose()
    {
        lock (_lock)
        {
            _disposed = true;
        }
        WaitForCompletion();
    }

    private async Task RunAsync(
        Func<CancellationToken, Task> operation,
        string operationName,
        CancellationToken cancellationToken)
    {
        try
        {
            await operation(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception ex)
        {
            _logger.Warn($"{operationName} failed", ex);
        }
    }
}
