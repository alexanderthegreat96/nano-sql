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