namespace NServiceBus.Routing.Partitioning
{
    using System;
    using System.Threading.Tasks;
    using NServiceBus;
    using Logging;
    using Pipeline;
    using Transport;

    class DistributeMessagesBasedOnHeader :
        IBehavior<IIncomingPhysicalMessageContext, IIncomingPhysicalMessageContext>
    {
        public DistributeMessagesBasedOnHeader(string localPartitionKey, Forwarder forwarder)
        {
            this.localPartitionKey = localPartitionKey;
            this.forwarder = forwarder;
        }

        public Task Invoke(IIncomingPhysicalMessageContext context, Func<IIncomingPhysicalMessageContext, Task> next)
        {
            var intent = context.Message.GetMesssageIntent();
            var isSubscriptionMessage =
                intent == MessageIntentEnum.Subscribe || intent == MessageIntentEnum.Unsubscribe;
            var isReply = intent == MessageIntentEnum.Reply;

            if (isSubscriptionMessage || isReply)
            {
                return next(context);
            }

            var hasPartitionKeyHeader =
                context.MessageHeaders.TryGetValue(PartitionHeaders.PartitionKey, out var messagePartitionKey);

            // 1. The header value isn't present (logical behavior will check message contents)
            // 2. The header value matches local partition key
            if (!hasPartitionKeyHeader || messagePartitionKey == localPartitionKey)
            {
                return next(context);
            }

            log.Debug($"##### Received message: {context.Message.Headers[Headers.EnclosedMessageTypes]} with Mapped PartitionKey={messagePartitionKey} on partition {localPartitionKey}");

            return forwarder.Forward(context, messagePartitionKey);
        }

        readonly string localPartitionKey;
        readonly Forwarder forwarder;
        static ILog log = LogManager.GetLogger<DistributeMessagesBasedOnHeader>();
    }
}