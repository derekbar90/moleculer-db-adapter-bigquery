# moleculer-db-adapter-bigquery

SQL adapter for Google BigQuery for Moleculer DB service with [Knex](https://github.com/knex/knex).

# Features
- Custom database resolver support
- Debug mode
- custom table resolver

# Install

```bash
npm install moleculer-db-adapter-bigquery
```

## Usage

Export the following environment variable

```
GOOGLE_APPLICATION_CREDENTIALS=/example/creds-3Af04f78f61a.json
```

Then you can connect by providing your project id within the bigQuery schema object

```js
"use strict";

const { ServiceBroker } = require("moleculer");
const DbService = require("moleculer-db");
const SqlAdapter = require("moleculer-db-adapter-sequelize");
const Sequelize = require("sequelize");

const broker = new ServiceBroker();

// Create a Sequelize service for `post` entities
broker.createService({
    name: "posts",
    mixins: [DbService],
    bigQuery: {
        getRegion: (ctx: Context) => Promise<BigQueryRegions | BigQueryMultiRegions>;
        getIdKey: (ctx?: Context) => Promise<string>;
        projectId: string;
        showLogs?: boolean;
    }
});


broker.start()
// Create a new post
.then(() => broker.call("posts.create", {
    title: "My first post",
    content: "Lorem ipsum...",
    votes: 0
}))

// Get all posts
.then(() => broker.call("posts.find").then(console.log));
```

### Raw queries
To run raw query use the following syntax:

```js
    actions: {
        findHello2() {
            return this.adapter.query("SELECT * FROM posts WHERE title = 'Hello 2' LIMIT 1", { location: 'US' })
                .then(([res, metadata]) => res);
        }
    }
```

# Test
```
$ npm test
```

In development with watching

```
$ npm run ci
```

# License
The project is available under the [MIT license](https://tldrlegal.com/license/mit-license).