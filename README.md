## NanoSQL
A simplified Python MySQL Wrapper. Simply a class that you use and then you forget about

It's based on https://github.com/knadh/simplemysql, therefore you will find all the documentation there. It's the same API, the difference is, it's a class not a package.


### Why?
I needed polling for the connections and I also fixed a couple of things. Nevertheless, I hate installing a ton of libraries, so I took it out of it's package, modified it and here we are.

#### Usage

```py
from nanosql import NanoSql
db = NanoSql(
    host="localhost",
    db="my-db",
    user="my-user",
    passwd="my-pass",
    port=3306,
    pool_size = 8
)

user = db.getOne("users", ("username=%s", ["username", "=", "alex"]))
print(user)
```