# orm

Small type-safe ORM for Python and PostgreSQL (asyncpg).

This is a very thin ORM, it still expects you to know how SQL works.

## Usage


### Creating tables
```python
import orm
from typing import Annotated
import asyncpg

class Account(orm.Table):
    id: Annotated[orm.Int, orm.primary()]
    username: orm.Text
    password: orm.Text

class Post(orm.Table):
    id: Annotated[orm.Int, orm.primary()]
    author: Annotated[orm.Int, orm.foreign(Account.id)]
    content: orm.Text
```
### Simple select

```python
async def find_account(db: asyncpg.Connection, username: str, password: str) -> Account | None:
    account = await (
        Account.select()
        .where(Accounts.email == email)
        .fetchone(db)
    )

    if not account:
        return

    if not verify_password(account.password, password):
        return

    return account
```

### Joins
```python
#                         types generated from the joins automatically
#                                                             ↓     ↓
async def get_all_posts(db: asyncpg.Connection) -> list[tuple[Post, Account]]:
    return await (
        Post.select()
        .join(Account.where(Post.author == Account.id))
        .fetch(db)
    )
```

### Inserts
```python
async def create_post(db: asyncpg.Connection, author: Account, content: str) -> Post:
    id = generate_id()

    return await (
        Post(id=id, author=author.id, content=content)
        .insert()
        .fetchone(db)
    )
```
