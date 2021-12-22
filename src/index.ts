/*
 * moleculer-db-adapter-sequelize
 * Copyright (c) 2019 MoleculerJS (https://github.com/moleculerjs/moleculer-db)
 * MIT Licensed
 */

"use strict";

import { BigQuery, JobOptions } from "@google-cloud/bigquery";
import { Knex, knex } from "knex";
import Moleculer, { Errors, Service, ServiceBroker } from "moleculer";
import { Model } from "sequelize";
import {
  BigQueryContext,
  BigQueryDbAdapterOptions
} from "./types/BigQuery";

const _ = require("lodash");
const Promise = require("bluebird");
const bq = knex({ client: "pg" });

class BigQueryDbAdapter {
  opts: any[];
  broker!: ServiceBroker;
  service!: Service;
  bigquery!: BigQuery;
  public bigQueryConfig!: BigQueryDbAdapterOptions;
  model!: typeof Model;

  /**
   * Creates an instance of BigQueryDbAdapter.
   * @param {any} opts
   *
   * @memberof BigQueryDbAdapter
   */
  constructor(...opts: any[]) {
    this.opts = opts || [];
  }

  /**
   * Initialize adapter
   *
   * @param {ServiceBroker} broker
   * @param {Service} service
   *
   * @memberof BigQueryDbAdapter
   */
  init(broker: ServiceBroker, service: Service) {
    this.broker = broker;
    this.service = service;
    this.bigquery = new BigQuery();

    if (!this.service.schema.bigQuery) {
      /* istanbul ignore next */
      throw new Errors.ServiceSchemaError(
        "Missing settings definition in schema of service! Please add bigQuery settings object.",
        null
      );
    }

    this.bigQueryConfig = this.service.schema.bigQuery;
  }

  /**
   * Connect to database
   *
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async connect() {
    this.service.logger.info(
      `BigQuery connected to projectId: ${this.bigQueryConfig?.projectId}`
    );
  }

  /**
   * Disconnect from database
   *
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async disconnect() {
    if (this.bigquery) {
      this.service.logger.info("Closing BigQuery instance connection");
    }
    /* istanbul ignore next */
    return Promise.resolve(true);
  }

  async retrieveContext(
    params: { [key: string]: any } | Array<{ [key: string]: any }>
  ) {
    if (Array.isArray(params)) {
      return [
        ...params.map((param) => {
          const context: BigQueryContext = param["adapter_private_context"];

          if (context == null) {
            throw new Moleculer.Errors.MoleculerError(
              "Unable to retrieve private context, please make sure you apply one via a hook."
            );
          }

          delete param.adapter_private_context;

          context.tableName = this.bigQueryConfig.getTableName(context)

          return context;
        }),
      ].shift();
    } else {
      const context: BigQueryContext = params["adapter_private_context"];

      if (context == null) {
        throw new Moleculer.Errors.MoleculerError(
          "Unable to retrieve private context, please make sure you apply one via a hook."
        );
      }

      delete params.adapter_private_context;

      context.tableName = this.bigQueryConfig.getTableName(context)

      return context;
    }
  }

  /**
   * Find all entities by filters.
   *
   * Available filter props:
   * 	- limit
   *  - offset
   *  - sort
   *  - search
   *  - searchFields
   *  - query
   *
   * @param {any} filters
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async find(filters: any) {
    return this.createCursor(filters, false);
  }

  formatQuery(query: string) {
    const formattedQuery = query.replace(/"/g, "`");
    this.bigQueryConfig.showLogs && this.service.logger.info(formattedQuery)
    return formattedQuery;
  }

  /**
   * Find an entity by query
   *
   * @param {Object} query
   * @returns {Promise}
   * @memberof MemoryDbAdapter
   */
  async findOne(query: Object) {
    const context = await this.retrieveContext(query);

    const compiledQuery = bq(`${context?.tableName}`)
      .where(query)
      .limit(1)
      .toString();

    const results = await this.query(this.formatQuery(compiledQuery), { location: context?.region });

    //@ts-ignore
    return results.shift();
  }

  /**
   * Find an entities by ID
   *
   * @param {any} _id
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async findById(context: BigQueryContext, id: string) {
    const parsedContext = await this.retrieveContext(context);

    const primaryKey = await this.bigQueryConfig.getIdKey();
    const compiledQuery = bq(`${parsedContext?.tableName}`)
      .where({
        [primaryKey]: id,
      })
      .limit(1)
      .toString();

    const results = await this.query(this.formatQuery(compiledQuery), { location: parsedContext?.region });

    return results.shift();
  }

  /**
   * Find any entities by IDs
   *
   * @param {Array} idList
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async findByIds(context: BigQueryContext, idList: Array<string>) {
    const parsedContext = await this.retrieveContext(context);

    const primaryKey = await this.bigQueryConfig.getIdKey();

    const compiledQuery = bq(`${parsedContext?.tableName}`)
      .whereIn(primaryKey, idList)
      .toString();

    const results = await this.query(this.formatQuery(compiledQuery), { location: parsedContext?.region });

    //@ts-ignore
    return results;
  }

  /**
   * Get count of filtered entities
   *
   * Available filter props:
   *  - search
   *  - searchFields
   *  - query
   *
   * @param {Object} [filters={}]
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async count(filters = {}) {
    return this.createCursor(filters, true);
  }

  /**
   * Insert an entity
   *
   * @param {Object} entity
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async insert(entity: { [key: string]: any }) {
    const parsedContext = await this.retrieveContext(entity);

    const primaryKey = await this.bigQueryConfig.getIdKey();

    const compiledQuery = `
      ${this.formatQuery(
        bq(`${parsedContext?.tableName}`)
          .insert({
            [primaryKey]: entity[primaryKey],
          })
          .toString()
      )};
      ${this.formatQuery(
        bq(`${parsedContext?.tableName}`)
          .whereIn(primaryKey, [entity[primaryKey]])
          .toString()
      )};
    `;

    const results = await this.query(this.formatQuery(compiledQuery), { location: parsedContext?.region });

    return results;
  }

  /**
   * Insert many entities
   *
   * @param {Array} entities
   * @param {Object} opts
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async insertMany(entities: Array<Object>, opts = { returning: true }) {
    const parsedContext = await this.retrieveContext(entities);

    if (parsedContext == null) {
      throw new Moleculer.Errors.MoleculerError(
        "Unable to retrieve private context, please make sure you apply one via a hook."
      );
    }

    const primaryKey = await this.bigQueryConfig.getIdKey();

    const compiledQuery = `
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`).insert(entities).toString()
      )};
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`)
          .whereIn(
            primaryKey,
            entities.map((entity: any) => entity[primaryKey])
          )
          .toString()
      )};
    `;

    const results = await this.query(this.formatQuery(compiledQuery), { location: parsedContext?.region });

    return results;
  }

  /**
   * Update many entities by `where` and `update`
   *
   * @param {Object} where
   * @param {Object} update
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async updateMany(where: Object, update: Object) {
    const parsedContext = await this.retrieveContext(update);

    if (parsedContext == null) {
      throw new Moleculer.Errors.MoleculerError(
        "Unable to retrieve private context, please make sure you apply one via a hook."
      );
    }

    const compiledQuery = `
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`)
          .where(where)
          .update(update)
          .toString()
      )};
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`).where(where).toString()
      )};
    `;

    const results = await this.query(this.formatQuery(compiledQuery), { location: parsedContext?.region });

    return results;
  }

  /**
   * Update an entity by ID and `update`
   *
   * @param {any} _id
   * @param {Object} update
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async updateById(id: string, update: Model) {
    const parsedContext = await this.retrieveContext(update);

    if (parsedContext == null) {
      throw new Moleculer.Errors.MoleculerError(
        "Unable to retrieve private context, please make sure you apply one via a hook."
      );
    }

    const primaryKey = await this.bigQueryConfig.getIdKey();

    const compiledQuery = `
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`)
          .where({
            [primaryKey]: id,
          })
          .update(update)
          .toString()
      )};
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`)
          .where({
            [primaryKey]: id,
          })
          .toString()
      )};
    `;

    const results = await this.query(this.formatQuery(compiledQuery), { location: parsedContext?.region });

    return results;
  }

  /**
   * Remove entities which are matched by `where`
   *
   * @param {Object} where
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async removeMany(where: Object) {
    const parsedContext = await this.retrieveContext(where);

    if (parsedContext == null) {
      throw new Moleculer.Errors.MoleculerError(
        "Unable to retrieve private context, please make sure you apply one via a hook."
      );
    }

    const compiledPreQuery = `
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`).where(where).toString()
      )};
    `;
    const compiledQuery = `
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`).where(where).del().toString()
      )};
    `;

    const entitiesToBeRemoved = await this.query(
      this.formatQuery(compiledPreQuery),
      { location: parsedContext?.region }
    );

    await this.query(this.formatQuery(compiledQuery), { location: parsedContext?.region });

    return entitiesToBeRemoved;
  }

  /**
   * Remove an entity by ID
   *
   * @param {any} _id
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  async removeById(context: BigQueryContext, _id: string) {
    const parsedContext = await this.retrieveContext(context);

    if (parsedContext == null) {
      throw new Moleculer.Errors.MoleculerError(
        "Unable to retrieve private context, please make sure you apply one via a hook."
      );
    }

    const primaryKey = await this.bigQueryConfig.getIdKey();

    const compiledPreQuery = `
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`)
          .where({
            [primaryKey]: _id,
          })
          .toString()
      )};
    `;
    const compiledQuery = `
      ${this.formatQuery(
        bq(`${parsedContext.tableName}`)
          .where({
            [primaryKey]: _id,
          })
          .del()
          .toString()
      )};
    `;

    const entitiesToBeRemoved = await this.query(
      this.formatQuery(compiledPreQuery),
      { location: parsedContext?.region }
    );

    await this.query(this.formatQuery(compiledQuery), { location: parsedContext?.region });

    return entitiesToBeRemoved;
  }

  /**
   * Clear all entities from collection
   *
   * @returns {Promise}
   *
   * @memberof BigQueryDbAdapter
   */
  // clear() {
  //   //@ts-ignore
  //   return this.model.destroy({ where: {} });
  // }

  /**
   * Convert DB entity to JSON object
   *
   * @param {any} entity
   * @returns {Object}
   * @memberof BigQueryDbAdapter
   */
  entityToObject(entity: Object) {
    let json = Object.assign({}, entity);
    return json;
  }

  private async  migrateIdParamToPrimaryKey(whereObject: { [key: string]: any }) {
    const whereObjectCopy = { ...whereObject };
    if(whereObjectCopy.id){
      const primaryKey = await this.bigQueryConfig.getIdKey();
      Object.assign(whereObjectCopy, {
        [primaryKey]: whereObjectCopy.id
      })
    }
    for(const backlistedKey of (this.bigQueryConfig.queryBlacklist || [])) {
      delete whereObjectCopy[backlistedKey]
    }
    return whereObjectCopy;
  }

  async createCursor(params: { [key: string]: any } = {}, isCounting?: boolean) {
    const context = await this.retrieveContext(params);
    let q = bq(`${context?.tableName}`);
    if (Object.keys(params || {}).length > 0) {
      // Full-text search
      if (_.isString(params?.search) && params?.search !== "") {
        let fields = [];
        if (params.searchFields) {
          fields = _.isString(params.searchFields)
            ? params.searchFields.split(" ")
            : params.searchFields;
        }

        if (params.query) {
          const whereParams = await this.migrateIdParamToPrimaryKey(params.query)
          q.where(whereParams);
        }

        if (fields.length > 0) {
          for (const field of fields) {
            q.orWhere(field, "like", `%${params.search}%`);
          }
        }

        if (params.sort) {
          this.transformSort(q, params.sort);
        }
      } else {
        const whereParams = await this.migrateIdParamToPrimaryKey(params.query)
        q.where(whereParams);
        // Sort
        if (params.sort) {
          this.transformSort(q, params.sort);
        }
      }

      // Offset
      if (_.isNumber(params.offset) && params.offset > 0) {
        q.offset(params.offset)
      }

      // Limit
      if (_.isNumber(params.limit) && params.limit > 0) {
        q.limit(params.limit)
      }
    }

    // If not params
    if (isCounting) {
      q.count(await this.bigQueryConfig.getIdKey());
    }
    else {
      q.where({});
    }

    const query = q.toString();

    const formattedQuery = this.formatQuery(query);
      
    const result = await this.query(formattedQuery, { location: context?.region })
    
    return isCounting ? result.shift()['f0_'] : result
  }

  /**
   * Convert the `sort` param to a `sort` object to Sequelize queries.
   *
   * @param {String|Array<String>|Object} paramSort
   * @returns {Object} Return with a sort object like `[["votes", "ASC"], ["title", "DESC"]]`
   * @memberof BigQueryDbAdapter
   */
  transformSort(q: Knex.QueryBuilder, paramSort: String | String[] | Object) {
    let sort = paramSort;
    if (_.isString(sort))
      //@ts-ignore
      sort = sort.replace(/,/, " ").split(" ");

    if (Array.isArray(sort)) {
      let sortObj = {};
      sort.forEach((s) => {
        if (s.startsWith("-"))
          //@ts-ignore
          sortObj[s.slice(1)] = -1;
        //@ts-ignore
        else sortObj[s] = 1;
      });
      sort = sortObj;
    }

    if (_.isObject(sort)) {
      Object.keys(sort).forEach((key) => {
        //@ts-ignore
        q.orderBy(key, sort[key] > 0 ? "asc" : "desc");
      });
    }

    return q;
  }

  /**
   * For compatibility only.
   * @param {Object} entity
   * @param {String} idField
   * @memberof BigQueryDbAdapter
   * @returns {Object} Entity
   */
  beforeSaveTransformID(entity: Object, idField: String) {
    return entity;
  }

  /**
   * For compatibility only.
   * @param {Object} entity
   * @param {String} idField
   * @memberof BigQueryDbAdapter
   * @returns {Object} Entity
   */
  afterRetrieveTransformID(entity: Object, idField: String) {
    return entity;
  }

  // async retrieveSchemaForTable(tableName: string) {
  //   const query = `
	// 	SELECT * EXCEPT(is_generated, generation_expression, is_stored, is_updatable)
	//    	FROM \`${this.bigQueryConfig.projectId}\`.${tableName}.INFORMATION_SCHEMA.COLUMNS
	//    	WHERE
	// 		table_name="compiled"`;

  //   //TODO: Need to grab location from config function
  //   return this.query(query, {}) as Promise<Array<TableSchemaFragment>>;
  // }

  // resolveSequelizeType(dataType: string) {
  //   switch (true) {
  //     case dataType.indexOf("FLOAT") > -1:
  //       return DataTypes.FLOAT;
  //     case dataType.indexOf("STRING") > -1:
  //       return DataTypes.STRING;
  //     case dataType.indexOf("TIMESTAMP") > -1:
  //       return DataTypes.DATE;
  //   }
  // }

  // convertModelFragmentToSequelizeModel(
  //   modelFragments: Array<TableSchemaFragment>
  // ) {
  //   return modelFragments.reduce((storage, fragment) => {
  //     return {
  //       ...storage,
  //       [fragment.column_name]: {
  //         type: this.resolveSequelizeType(fragment.data_type),
  //         allowNull: Boolean(fragment.is_nullable),
  //       },
  //     };
  //   }, {});
  // }

  // async getModel(context: BigQueryContext) {
  //   const tableName = `Impact_${context.impact.replace(/-/g, "_")}`;

  //   const currentModel = await this.retrieveSchemaForTable(tableName);

  //   const model = this.convertModelFragmentToSequelizeModel(currentModel);

  //   const adapter = new Sequelize(
  //     `postgres://${
  //       process.env.USE_TIMESCALE === "true"
  //         ? process.env.TIMESCALE_USER
  //         : process.env.POSTGRES_USER
  //     }:${
  //       process.env.USE_TIMESCALE === "true"
  //         ? process.env.TIMESCALE_PASSWORD
  //         : process.env.POSTGRES_PASSWORD
  //     }@${
  //       process.env.USE_TIMESCALE === "true"
  //         ? process.env.TIMESCALE_HOST
  //         : process.env.POSTGRES_HOST
  //     }:${
  //       process.env.USE_TIMESCALE === "true"
  //         ? process.env.TIMESCALE_PORT
  //         : process.env.POSTGRES_PORT
  //     }/${process.env.DB_NAME}`
  //   );

  //   const sqlModel = adapter.define(tableName, model, {});

  //   return sqlModel;
  // }

  async query(query: string, queryOptions: JobOptions) {
    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query

    let formattedQuery = this.bigQueryConfig.queryWrapper ? this.bigQueryConfig.queryWrapper(query, queryOptions.location || 'US') : query;

    const options = {
      query: formattedQuery,
      // Location must match that of the dataset(s) referenced in the query.
      location: "US",
      ...queryOptions,
    };

    // Run the query as a job
    const [job] = await this.bigquery.createQueryJob(options);

    // Log what is being run
    this.service.logger.info(`Job ${job.id} started for query ${query}`);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();

    // Log what is being run
    this.service.logger.info(`Job ${job.id} complete`);

    return rows;
  }
}

module.exports = BigQueryDbAdapter;