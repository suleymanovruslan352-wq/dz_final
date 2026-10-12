Random random = new Random();



List<Order> orders = new List<Order>();
object lockObject = new object();
SemaphoreSlim semaphore = new SemaphoreSlim(2, 2);

int count_order = 0;
decimal sum = 0;
string filePath = "results.txt";


for (int i = 1; i <= 100; i++)
{
orders.Add(new Order
{
    Id = i,
    Price = random.Next(100, 1000),
    IsProcessed = false
});
}

List<Thread> threads = new List<Thread>();
Queue<Order> ordersQueue = new Queue<Order>(orders);


for (int i = 0; i < 5; i++)
{
        Thread thread = new Thread(() =>
        {
            while (Order._isRunning) 
            {
                Order order = null;
                bool lockTaken = false;

                try
                {
                    Monitor.Enter(lockObject, ref lockTaken);
                    if (ordersQueue.Count > 0)
                    {
                        order = ordersQueue.Dequeue();
                        count_order++;
                        sum = sum + order.Price;

                        if (ordersQueue.Count == 0)
                        {
                            Order._isRunning = false;
                        }
                    }
                }
                finally
                {
                    if (lockTaken)
                    {
                        Monitor.Exit(lockObject);
                    }
                }

                if (order == null)
                {
                break;
                }


                semaphore.Wait();
                try
                {
                    order.IsProcessed = true;
                    string logMessage = $"Поток {Thread.CurrentThread.ManagedThreadId} Обработан заказ ID: {order.Id}, Стоимость: {order.Price}\n";
                    Console.Write(logMessage);
                    lock (lockObject)
                    {
                        File.AppendAllText(filePath, logMessage);
                    }
                    Thread.Sleep(200);

                }
                finally
                {
                    semaphore.Release(); 
                }
            }
        });

        threads.Add(thread);
        thread.Start();

    

    

}




foreach (Thread thread in threads)
    thread.Join();

Console.WriteLine(count_order);
Console.WriteLine(sum);
class Order
{
    public int Id { get; set; }
    public decimal Price { get; set; }
    public bool IsProcessed { get; set; }

    public static volatile bool _isRunning = true;


}