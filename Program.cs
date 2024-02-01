using System;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Xml;
using System.Globalization;

namespace XmlToDatabase
{
    class Program
    {
        static void Main(string[] args)
        {
            // Создание конфигурации из файла config.json
            IConfiguration config = new ConfigurationBuilder()
            .AddJsonFile("config.json", optional: true, reloadOnChange: true)
            .Build();

            // Получаем строку подключения
            string? connectionString = config.GetConnectionString("DefaultConnection");

            // Путь к XML файлу
            string? xmlFilePath = config["PathToXML"];

            if (!File.Exists(xmlFilePath))
            {
                Console.WriteLine($"Файл XML не найден по пути: {xmlFilePath}");
                return;
            }

            // Создание подключения к базе данных
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // Загрузка XML файла
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load(xmlFilePath);

                // Парсинг XML и вставка данных в базу данных
                XmlNodeList orderList = xmlDoc.SelectNodes("//order");
                foreach (XmlNode orderNode in orderList)
                {
                    int orderId = int.Parse(orderNode.SelectSingleNode("no").InnerText);
                    string regDate = orderNode.SelectSingleNode("reg_date").InnerText;
                    decimal sum = decimal.Parse(orderNode.SelectSingleNode("sum").InnerText, CultureInfo.InvariantCulture);
                    string fio = orderNode.SelectSingleNode("user/fio").InnerText;
                    string email = orderNode.SelectSingleNode("user/email").InnerText;


                    // Вставка информации о покупателе
                    // Проверка наличия покупателя в базе данных
                    SqlCommand checkCustomerCommand = new SqlCommand("SELECT ID FROM Customers " +
                        "WHERE Name = @Name AND [e-mail] = @Mail;", connection);
                    checkCustomerCommand.Parameters.AddWithValue("@Name", fio);
                    checkCustomerCommand.Parameters.AddWithValue("@Mail", email);
                    int сustomerId = Convert.ToInt32(checkCustomerCommand.ExecuteScalar());
                    if (сustomerId == 0)
                    {
                        SqlCommand insertCustomerCommand = new SqlCommand("INSERT INTO Customers (Name, [e-mail]) " +
                            "VALUES (@Name, @Mail);", connection);
                        insertCustomerCommand.Parameters.AddWithValue("@Name", fio);
                        insertCustomerCommand.Parameters.AddWithValue("@Mail", email);
                        insertCustomerCommand.ExecuteNonQuery();

                        SqlCommand selectCustomerIdCommand = new SqlCommand("SELECT IDENT_CURRENT( 'Customers' );", connection);
                        сustomerId = Convert.ToInt32(selectCustomerIdCommand.ExecuteScalar());
                    }

                    // Вставка информации о заказе в базу данных
                    // Отключение параметра IDENTITY_INSERT
                    // (т.к. изначально в таблице идентификатор генерируется автоматически)
                    SqlCommand disableIdentityInsert = new SqlCommand("SET IDENTITY_INSERT Purchases ON;", connection);
                    disableIdentityInsert.ExecuteNonQuery();

                    SqlCommand insertOrderCommand = new SqlCommand("INSERT INTO Purchases (ID, Customer_ID, Purchase_date, Total_cost) " +
                        "VALUES (@ID, @CustomerID, @RegDate, @Sum);", connection);
                    insertOrderCommand.Parameters.AddWithValue("@RegDate", regDate);
                    insertOrderCommand.Parameters.AddWithValue("@ID", orderId);
                    insertOrderCommand.Parameters.AddWithValue("@Sum", sum);
                    insertOrderCommand.Parameters.AddWithValue("@CustomerID", сustomerId);
                    insertOrderCommand.ExecuteNonQuery();

                    // Включение параметра IDENTITY_INSERT обратно
                    SqlCommand enableIdentityInsert = new SqlCommand("SET IDENTITY_INSERT Purchases OFF;", connection);
                    enableIdentityInsert.ExecuteNonQuery();

                    // Получение информации о товарах в заказе
                    XmlNodeList productList = orderNode.SelectNodes("product");
                    // Вставка информации о товарах и о товарах в заказе
                    foreach (XmlNode productNode in productList)
                    {
                        int quantity = int.Parse(productNode.SelectSingleNode("quantity").InnerText);
                        string productName = productNode.SelectSingleNode("name").InnerText;
                        decimal price = decimal.Parse(productNode.SelectSingleNode("price").InnerText, CultureInfo.InvariantCulture);

                        // Проверка наличия продукта в базе данных
                        SqlCommand checkProductCommand = new SqlCommand("SELECT ID FROM Products WHERE Name = @Name " +
                            "AND Start_price = @Price;", connection);
                        checkProductCommand.Parameters.AddWithValue("@Name", productName);
                        checkProductCommand.Parameters.AddWithValue("@Price", price);
                        int productId = Convert.ToInt32(checkProductCommand.ExecuteScalar());

                        // Если продукт уже существует, пропустить вставку
                        if (productId == 0)
                        {
                            SqlCommand insertProductCommand = new SqlCommand("INSERT INTO Products (Name, Start_price) " +
                                "VALUES (@Name, @Price);", connection);
                            insertProductCommand.Parameters.AddWithValue("@Name", productName);
                            insertProductCommand.Parameters.AddWithValue("@Price", price);
                            insertProductCommand.ExecuteNonQuery();

                            SqlCommand selectProductIdCommand = new SqlCommand("SELECT IDENT_CURRENT( 'Products' );", connection);
                            productId = Convert.ToInt32(selectProductIdCommand.ExecuteScalar());
                        }

                        // Добавление информации о товаре в заказе
                        SqlCommand insertProductInOrderCommand = new SqlCommand("INSERT INTO [Purchases Item] (Purchase_ID, Product_ID, Count, Price) " +
                            "VALUES (@OrderId, @ProductId, @Quantity, @Price);", connection);
                        insertProductInOrderCommand.Parameters.AddWithValue("@OrderId", orderId);
                        insertProductInOrderCommand.Parameters.AddWithValue("@ProductId", productId);
                        insertProductInOrderCommand.Parameters.AddWithValue("@Quantity", quantity);
                        insertProductInOrderCommand.Parameters.AddWithValue("@Price", price);
                        insertProductInOrderCommand.ExecuteNonQuery();
                    }
                }

                Console.WriteLine("Данные успешно загружены в базу данных.");
            }
        }
    }
}