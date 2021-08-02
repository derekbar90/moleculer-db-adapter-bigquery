"use strict";

const { ServiceBroker, Context } = require("moleculer");

jest.mock("sequelize");

jest.setTimeout(30000);

const model = {
  sync: jest.fn(() => Promise.resolve()),
  findAll: jest.fn(() => Promise.resolve()),
  count: jest.fn(() => Promise.resolve()),
  findOne: jest.fn(() => Promise.resolve()),
  findByPk: jest.fn(() => Promise.resolve()),
  create: jest.fn(() => Promise.resolve()),
  bulkCreate: jest.fn(() => Promise.resolve()),
  update: jest.fn(() => Promise.resolve([1, 2])),
  destroy: jest.fn(() => Promise.resolve()),
};

const db = {
  authenticate: jest.fn(() => Promise.resolve()),
  define: jest.fn(() => model),
  close: jest.fn(() => Promise.resolve()),
};

let Sequelize = require("sequelize");
const Op = Sequelize.Op;

Sequelize.mockImplementation(() => db);

const SequelizeAdapter = require("../../src");

function protectReject(err) {
  if (err && err.stack) {
    console.error(err);
    console.error(err.stack);
  }
  expect(err).toBe(true);
}

const fakeModel = {
  name: "posts",
  define: {
    a: 5,
  },
  options: {
    b: 10,
  },
};
const initiatedModel = {
  attributes: {},
};

let fakeConn = Promise.resolve();
fakeConn.connection = {
  on: jest.fn(),
  close: jest.fn(),
};

const bigQueryConfig = {
  projectId: "proof-of-impact",
  getRegion: async () => {
    return "US";
  },
  getIdKey: async () => {
    return "Contacto__c";
  },
  getTableName: (context) => {
    return `Impact_${context.impact.replace(/-/g, "_")}.compiled`
  }
};
const serviceHooks = {
  before: {
    create: [
      function addTimestamp(ctx) {
        // Add timestamp
        ctx.params.createdAt = new Date();
        return ctx;
      },
    ],
  },
};

const adapter_private_context = {
  orgId: "58123dfd-cbcc-4350-8a49-24ee663d6db3",
  impact: "9e32ebd8-5d74-47c8-b69b-0bf655134bed",
};

const manageParams = (params) => {
  if (Array.isArray(params)) {
    return params.map((param) => {
      return {
        adapter_private_context: adapter_private_context,
        ...param,
      };
    });
  } else {
    return {
      adapter_private_context: adapter_private_context,
      ...params,
    };
  }
};

describe("Test BigQueryAdapter", () => {
  beforeEach(() => {
    Sequelize.mockClear();
    db.authenticate.mockClear();
    db.define.mockClear();
    model.sync.mockClear();
  });

  describe("model definition as description", () => {
    const opts = {
      dialect: "sqlite",
    };
    const adapter = new SequelizeAdapter(opts);

    const broker = new ServiceBroker({ logger: false });
    const service = broker.createService({
      name: "store",
      model: fakeModel,
      bigQuery: bigQueryConfig,
      hooks: serviceHooks,
    });

    beforeEach(() => {
      adapter.init(broker, service);
    });

    it("should be created", () => {
      expect(adapter).toBeDefined();
      expect(adapter.opts).toEqual([opts]);
      expect(adapter.init).toBeDefined();
      expect(adapter.connect).toBeDefined();
      expect(adapter.disconnect).toBeDefined();
      expect(adapter.find).toBeDefined();
      expect(adapter.findOne).toBeDefined();
      expect(adapter.findById).toBeDefined();
      expect(adapter.findByIds).toBeDefined();
      expect(adapter.count).toBeDefined();
      expect(adapter.insert).toBeDefined();
      expect(adapter.insertMany).toBeDefined();
      expect(adapter.updateMany).toBeDefined();
      expect(adapter.updateById).toBeDefined();
      expect(adapter.removeMany).toBeDefined();
      expect(adapter.removeById).toBeDefined();
      // expect(adapter.clear).toBeDefined();
      expect(adapter.beforeSaveTransformID).toBeInstanceOf(Function);
      expect(adapter.afterRetrieveTransformID).toBeInstanceOf(Function);
    });

    it("call init", () => {
      expect(adapter.broker).toBe(broker);
      expect(adapter.service).toBe(service);
      expect(adapter.bigQueryConfig).not.toBeNull();
    });

    it("call connect with uri", () => {
      return adapter
        .connect()
        .catch(protectReject)
        .then(() => {
          expect(adapter.service.schema.bigQuery).not.toBeNull();
        });
    });

    it("should disconnect after connection error", () => {
      let hasThrown = true;
      model.sync.mockImplementationOnce(() => Promise.reject());
      return adapter
        .connect()
        .then(() => {
          hasThrown = false;
        })
        .catch(() => {
          expect(hasThrown).toBe(true);
          expect(adapter.db.close).toBeCalledTimes(1);
        });
    });

    it("call disconnect", () => {
      return adapter
        .disconnect()
        .catch(protectReject)
        .then((result) => {
          expect(result).toBeTruthy();
        });
    });

    describe("Test createCursor", () => {
      it("call with only context", async () => {
        const params = manageParams({});
        const result = await adapter.createCursor(params);
        expect(result.length).toBeGreaterThan(1);
      });

      it("call without params as counting", async () => {
        const params = manageParams({});
        const result = await adapter.createCursor(params, true);
        expect(result).toBeGreaterThan(100);
      });

      it("call with query", async () => {
        let query = {};
        const params = manageParams({ query });
        const result = await adapter.createCursor(params);
        expect(result.length).toBeGreaterThan(100);
      });

      it("call with query & counting", async () => {
        let query = {};
        const params = manageParams({ query });
        const result = await adapter.createCursor(params, true);
        expect(result).toBeGreaterThan(100);
      });

      it("call with sort string", async () => {
        let query = {};
        const params = manageParams({
          query,
          sort: "-Contacto__c",
        });

        const result = await adapter.createCursor(params);
        expect(result[0].Contacto__c).toEqual("9691");
        expect(result[1].Contacto__c).toEqual("9524");
      });

      it("call with sort array", async () => {
        let query = {};
        const params = manageParams({
          query,
        });
        const result = await adapter.createCursor({ ...params, sort: ["Contacto__c", "RANGO_EDAD"] });
        expect(result[0].Contacto__c).toEqual("0035A00003PL0ODQA1");
        expect(result[1].Contacto__c).toEqual("0035A00003PL0OEQA1");
      });

      it("call with sort object", async () => {
        let query = {};
        const params = manageParams({
          query,
        });
        
        const result = await adapter.createCursor({ ...params, sort: { Contacto__c: 1, RANGO_EDAD: -1 } });
        expect(result[0].Contacto__c).toEqual("0035A00003PL0ODQA1");
        expect(result[1].Contacto__c).toEqual("0035A00003PL0OEQA1");
      });

      it("call with limit & offset", async () => {
        let query = {};
        const params = manageParams({
          query,
          sort: { Contacto__c: 1, RANGO_EDAD: -1 }
        });
        
        const result = await adapter.createCursor({ ...params, limit: 1, offset: 1 });
        expect(result[0].Contacto__c).toEqual("0035A00003PL0OEQA1");
      });

      it("call with full-text search without query", async () => {
        let query = {};
        const params = manageParams({
          query,
          sort: { Contacto__c: 1, RANGO_EDAD: -1 }
        });
        const result = await adapter.createCursor({
          ...params,
          search: "0035A00003PL0OEQA1",
          searchFields: ["Contacto__c", "RANGO_EDAD"],
        });
        expect(result[0].Contacto__c).toEqual("0035A00003PL0OEQA1");
      });

      it("call with full-text search with query", async () => {
        let query = { COMORA_MAX_CLIE: 7 };
        const params = manageParams({
          query,
          sort: { Contacto__c: 1, RANGO_EDAD: -1 }
        });
        const result = await adapter.createCursor({
          ...params,
          search: "0035A00003PL0OE",
          searchFields: ["Contacto__c", "RANGO_EDAD"],
        });
        expect(result[0].Contacto__c).toEqual("0035A00003PL0OEQA1");
        expect(result[0].COMORA_MAX_CLIE).toEqual(7);
      });

      // it("call with full-text search & advanced query", () => {
      //   adapter.model.findAll.mockClear();
      //   adapter.createCursor({
      //     query: {
      //       [Op.or]: [{ status: 1 }, { deleted: 0 }],
      //     },
      //     search: "walter",
      //     searchFields: ["title", "content"],
      //   });
      //   expect(adapter.model.findAll).toHaveBeenCalledTimes(1);
      //   expect(adapter.model.findAll).toHaveBeenCalledWith({
      //     where: {
      //       [Op.and]: [
      //         { [Op.or]: [{ status: 1 }, { deleted: 0 }] },
      //         {
      //           [Op.or]: [
      //             {
      //               title: {
      //                 [Op.like]: "%walter%",
      //               },
      //             },
      //             {
      //               content: {
      //                 [Op.like]: "%walter%",
      //               },
      //             },
      //           ],
      //         },
      //       ],
      //     },
      //   });
      // });
    });

    it("call find", () => {
      const params = manageParams({});
      return adapter
        .find(params)
        .catch(protectReject)
        .then((result) => {
          expect(result.length).toBeGreaterThan(0);
        });
    });

    it("call findOne", () => {
      let age = { COMORA_MAX_CLIE: 3 };
      const params = manageParams(age);
      return adapter
        .findOne(params)
        .catch(protectReject)
        .then((result) => {
          expect(typeof result).toEqual("object");
        });
    });

    it("call findByPk", () => {
      // adapter.findById.mockClear();

      const params = manageParams({});

      return adapter
        .findById(params, "0035A00003XiSdEQAV")
        .catch(protectReject)
        .then((result) => {
          expect(typeof result).toEqual("object");
        });
    });

    it("call findByIds", () => {
      // adapter.model.findAll.mockClear();
      const params = manageParams({});

      return adapter
        .findByIds(params, ["0035A00003XiSdEQAV"])
        .catch(protectReject)
        .then((results) => {
          expect(results.length).toBe(1);
        });
    });

    it("call count", () => {
      let origParams = {};

      const params = manageParams(origParams);

      return adapter
        .count(params)
        .catch(protectReject)
        .then((result) => {
          expect(result).toBeGreaterThan(30000);
        });
    });

    it("call insert", () => {
      let entity = {
        Contacto__c: Number(Math.random() * 100000).toFixed(0),
      };
      const params = manageParams(entity);
      return adapter
        .insert(params)
        .catch(protectReject)
        .then((result) => {
          expect(result.length).toEqual(1);
        });
    });

    it("call inserts", () => {
      let entities = [
        { Contacto__c: Number(Math.random() * 100000).toFixed(0) },
        { Contacto__c: Number(Math.random() * 100000).toFixed(0) },
      ];

      const params = manageParams(entities);

      return adapter
        .insertMany(params)
        .catch(protectReject)
        .then((results) => {
          expect(results.length).toEqual(2);
        });
    });

    // it("call inserts with option param", () => {
    //   adapter.model.create.mockClear();
    //   let entities = [{ name: "John" }, { name: "Jane" }];
    //   let opts = { ignoreDuplicates: true, returning: false };

    //   return adapter
    //     .insertMany(entities, opts)
    //     .catch(protectReject)
    //     .then(() => {
    //       expect(adapter.model.bulkCreate).toHaveBeenCalledTimes(2);
    //       expect(adapter.model.bulkCreate).toHaveBeenCalledWith(entities, opts);
    //     });
    // });

    it("call updateMany", () => {
      let where = {
        Contacto__c: "0035A00003XiSdEQAV",
      };
      let update = {
        RANGO_EDAD: `0-${Number(Math.random() * 100000).toFixed(0)}`,
      };

      const params = manageParams(update);

      return adapter
        .updateMany(where, params)
        .catch(protectReject)
        .then((res) => {
          expect(res.length).toBe(1);
        });
    });

    it("call updateById", () => {
      const id = "0035A00003XiSdEQAV";
      let update = {
        RANGO_EDAD: `0-${Number(Math.random() * 100000).toFixed(0)}`,
      };

      const params = manageParams(update);

      return adapter
        .updateById(id, params)
        .catch(protectReject)
        .then((result) => {
          expect(result.length).toEqual(1);
        });
    });

    it("call destroy", async () => {
      const id = Number(Math.random() * 1000).toFixed(0);

      let entity = {
        Contacto__c: id,
      };

      let params = manageParams(entity);
      const inserted = await adapter.insert(params);
      let params2 = manageParams(entity);
      return adapter
        .removeMany(params2)
        .catch(protectReject)
        .then((result) => {
          expect(result.length).toEqual(inserted.length);
        });
    });

    it("call entity.destroy", async () => {
      const id = Number(Math.random() * 1000).toFixed(0);

      let entity = {
        Contacto__c: id,
      };

      let params = manageParams(entity);
      let params2 = manageParams(entity);
      await adapter.insert(params);

      return adapter
        .removeById(params2, id)
        .catch(protectReject)
        .then((res) => {
          expect(res.length).toBe(1);
        });
    });

    // it("call clear", () => {
    //   return adapter
    //     .clear()
    //     .catch(protectReject)
    //     .then(() => {
    //       expect(adapter.model.destroy).toHaveBeenCalledTimes(1);
    //       expect(adapter.model.destroy).toHaveBeenCalledWith({ where: {} });
    //     });
    // });

    it("call doc.toJSON", () => {
      let doc = {
        proptery: "something",
      };

      expect(adapter.entityToObject(doc)).toEqual(doc);
    });

    it("should transform idField into _id", () => {
      let entry = {
        myID: "123456789",
        title: "My first post",
      };
      let idField = "myID";
      let res = adapter.beforeSaveTransformID(entry, idField);
      expect(res).toEqual(entry);
    });

    it("should transform _id into idField", () => {
      let entry = {
        _id: "123456789",
        title: "My first post",
      };
      let idField = "myID";
      let res = adapter.afterRetrieveTransformID(entry, idField);
      expect(res).toEqual(entry);
    });
  });
});
