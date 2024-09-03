### Kafka Updates Aggregator

A project to aggregate updates happening in `Kafka` across different topics into new user defined topics.

Let's say, your app generates some events, e.g. users can `login`, `deposit` and `withdraw` assets and there're relevant topics with pseudo schemas:
```shell
user_login
  user_id:    string
  login_time: string
```

```shell
deposit
  user_id:         string
  balance:         int
  deposit:         int
  isAuthenticated: bool
  country:         string
```

```shell
 withdrawal
   user_id:    string
   withdrawal: int
```

But what if we want to have messages with some info aggregated across the topics upon some update? E.g. we want to embed `balance` data into `login` event:
```shell
login_info
  user_id:    string
  login_time: string
  balance:    int
```
Or maybe we want to aggregate `balance` updates event? No problem:
```shell
balance_updates
  user_id:    string
  balance:    int
  deposit:    int
  withdrawal: int
```

Leverage REST API to submit data schema with field names and subject (to feed it to internal `Schema Registry`) and start aggregating data of your platform.
