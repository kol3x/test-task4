const mysql = require("mysql")

const connection = mysql.createConnection({
  host: "81.31.247.100",
  port: 3306,
  user: "yMYNdT",
  password: "woDyQpAhbRxmHCWJ",
  database: "testdatabase",
})

const MAX_CLIENTS_PER_MANAGER = 3000
const MIN_DATE = 1717362000

let customerLedToFirstOrder = 0

connection.connect((err) => {
  if (err) {
    console.error("Error connecting: " + err.stack)
    return
  }
  console.log("Connected as id " + connection.threadId)
  assignAllCustomersToManagers().finally(() => {
    connection.end()
  })
})

async function assignAllCustomersToManagers() {
  try {
    const customers = await query(
      "SELECT customers.id AS customer_id, customers.city_id " +
      "FROM customers " +
      "LEFT JOIN customer_to_manager_assign ON customers.id = customer_to_manager_assign.customer_id " +
      "WHERE customer_to_manager_assign.customer_id IS NULL"
    );

    for (const customer of customers) {
      await assignManager(customer)
    }

    console.log("Customer assignment completed.")
    console.log(
      "Customers led to first order by attraction managers ",
      customerLedToFirstOrder
    )
  } catch (error) {
    console.error(error)
  }
}

async function assignManager(customer) {
  if (!customer.first_order_date) {
    await assignToAttractionManager(customer)
  } else if (customer.first_order_date > MIN_DATE) {
    await assignToSupportManager(customer)
  }
  return
}

async function assignToAttractionManager(customer) {
  const managers = await query(
    "SELECT id FROM managers WHERE attached_clients_count <= ? AND role = 'Менеджер по привлечению' ORDER BY efficiency DESC LIMIT 1",
    [MAX_CLIENTS_PER_MANAGER]
  )
  const manager = managers[0]
  await query(
    "INSERT INTO customer_to_manager_assign (customer_id, city_id, manager_id, created_at) VALUES (?, ?, ?, NOW())",
    [customer.id, customer.city_id, manager.id]
  )
  await updateManagerClientCount(manager.id, 1)
}

async function assignToSupportManager() {
  const managers = await query(
    "SELECT id FROM managers WHERE attached_clients_count <= ? AND role = 'Менеджер поддержки' ORDER BY efficiency DESC LIMIT 1",
    [MAX_CLIENTS_PER_MANAGER]
  )
  const manager = managers[0]

  const existingRelation = query(
    "SELECT * FROM customer_to_manager_assign" +
      "WHERE customer_id = ? AND city_id = ?",
    [customer.id, customer.city_id]
  )
  if (existingRelation.length > 0) {
    customerLedToFirstOrder++
    await query(
      "UPDATE customer_to_manager_assign SET manager_id = ? " +
        "WHERE customer_id = ? AND city_id = ?",
      [manager.id, customer.id, customer.city_id]
    )
    await updateManagerClientCount(manager.id, -1)
  } else {
    await query(
      "INSERT INTO customer_to_manager_assign (customer_id, city_id, manager_id, created_at) VALUES (?, ?, ?, NOW())",
      [customer.id, customer.city_id, manager.id]
    )
    await updateManagerClientCount(manager.id, 1)
  }
}

async function updateManagerClientCount(managerId, num) {
  await query(
    "UPDATE managers SET attached_clients_count = attached_clients_count + ? WHERE id = ?",
    [num, managerId]
  )
}

function query(sql, params) {
  return new Promise((resolve, reject) => {
    connection.query(sql, params, (error, results) => {
      if (error) {
        return reject(error)
      }
      resolve(results)
    })
  })
}
