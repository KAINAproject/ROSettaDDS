using ROSettaDDS.Common.Logging;

namespace ROSettaDDS.Rtps.Writer;

/// <summary>
/// StatefulWriter の非同期タスク追跡と終了待機を担当する内部クラス。
/// </summary>
internal sealed class BackgroundOperationTracker
{
    private readonly object _lock = new();
    private readonly HashSet<Task> _tasks = new();
    private readonly ILogger _logger;

    public BackgroundOperationTracker(ILogger? logger = null)
    {
        _logger = logger ?? NullLogger.Instance;
    }

    public void Run(
        Func<CancellationToken, Task> operation,
        string operationName,
        CancellationToken cancellationToken)
    {
        var task = RunAsync(operation, operationName, cancellationToken);
        lock (_lock)
        {
            _tasks.Add(task);
        }

        _ = task.ContinueWith(
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
                _logger.Warn("StatefulWriter background tasks did not exit cleanly");
            }
        }
        catch (AggregateException ex) when (ex.InnerExceptions.All(e => e is OperationCanceledException))
        {
        }
        catch (Exception ex)
        {
            _logger.Warn("StatefulWriter background tasks did not exit cleanly", ex);
        }
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
