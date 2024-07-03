A project to aggregate updates happening in Kafka across different topics into new user defined topics.

Let's say, your app generates some events, e.g. users can login, deposit and withdraw assets and there're relevant topics with pseudo schemas:
```
user_login
  user_id: string
  login_time: string
```

```
deposit
  user_id: string
  balance         int
	deposit         int
	isAuthenticated bool
	country         string
```

```
 withdrawal
  user_id: string
  withdrawal: int
```

But what if we want to have messages with some info aggregated across the topics upon some update? E.g. we want login_and/or_balance updates info events:
```
login_info
  user_id: string
  login_time: string
  balance         int
```
Or balance updates event? No problem:
```
balance_updates
  user_id: string
  balance         int
  deposit         int
  withdrawal: int
```

Leverage REST API to submit data schema with field names and subject (to feed it to internal Schema Registry) and start aggregating data of your platform.
