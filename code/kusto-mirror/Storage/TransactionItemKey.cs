namespace Kusto.Mirror.ConsoleApp.Storage
{
    internal record TransactionItemKey(
        int StartTxId,
        int EndTxId,
        TransactionItemAction Action,
        string? BlobPath);
}