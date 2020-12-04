# Mysql Container

```bash
docker exec -it 6ff2b20c1d5c  bash
mysql -u root -p
use mysql;
show tables;
```

## Create connection

- see dag for connection name:  `mysql_conn_id="mysql_conn"` 
- create new connection `mysql_conn`
- connection type `MySQL, `host `mysql`, schema `mysql`, login `root`, password

## Troubleshooting

```
cryptography.fernet.InvalidToken
```

- check the connection and repair it if needed.

