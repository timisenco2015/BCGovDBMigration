const { Client } = require('pg');
const config = require('config');
const winstonLogger = require('../wistonConfig');
class DatabaseMigration {
  constructor() {
    
    this.client = new Client({
      user: config.get('sourcedb.username'),
      host: config.get('sourcedb.host'),
      database: config.get('sourcedb.database'),
      password: config.get('sourcedb.password'),
      port: config.get('sourcedb.port')
    });

    this.client2 = new Client({
      user: config.get('destinationdb.username'),
      host: config.get('destinationdb.host'),
      database: config.get('destinationdb.database'),
      password: config.get('destinationdb.password'),
      port: config.get('destinationdb.port')
    });

    this.client.connect();
    this.client2.connect();

    this.isError = false;
    this.insertRecord={};
  }

  _extraColumnIndex(data) {
    let values ='';
    if (data) {
      for(let index in Object.keys(data[0])) {
        let indexes=parseInt(index)+1;
        values =values+"$"+(indexes)+", "; 
      }
    }
    values = values.replace(/,\s*$/, "");
    return values;
  }
  _extraRows(data) {
    let rows = '';
    for (let row of Object.keys(data)) {
      rows = rows+`"${row}", `
    }
    rows = rows.replace(/,\s*$/, "");
    return rows;
  }

  //this will delete all the data in all the tables. This is to prevent
  // unique constrainsts in all the tables
  async truncateTables() {
    let tableNamesA = config.get('beforeTableNames');
    let tableNamesB = config.get('afterTableNames');
    if(tableNamesA && tableNamesB) {
      let tableNamesConc = [...tableNamesB,"user", ...tableNamesA, ];
      await this.client2.query('BEGIN');
      for (let tableName of tableNamesConc) {
        await this.client2
          .query(`DELETE FROM "${tableName}" cascade`)
          .then(async res => {
            winstonLogger.info("All tables truncated");
            return res.rows[0];
          })
          .catch(async e => {
            winstonLogger.error(e); 
            throw new Error(e);
          });
      }
      console.log("----------------------------");
      console.log("all tables truncated");
      console.log("----------------------------");
      await this.client2.query('COMMIT');       
    }
  }

  //calls this method to start migration
  async migrate(users) {
    
    await this.client2.query('BEGIN');

    //since user table will be altered, we need migrate the all parent tables
    //to user table
    let tableNames=config.get('beforeTableNames');
    await this.migrateTables(tableNames);

    //migrate user table
    await this.migrateUserTable(users);

    //migrate tables that are dependent on user table and other tables 
    tableNames=config.get('afterTableNames');
    await this.migrateTables(tableNames);
    
    await this.client2.query('COMMIT');
    
    if (!this.error) {
      this.client.end();
      this.client2.end();
      winstonLogger.info(this.insertRecord); 
      console.log(this.insertRecord);
      winstonLogger.log('error', e);
      console.log("----------------------------------------");
      console.log("Connection now closed");
      console.log("----------------------------------------");
    }
  }

  async migrateUserTable(users) {
    try {
      if(users) {
        this.insertRecord['user']=[];
        for await(let user of users) {
          let editedUser = await this.selectFromUserTable(user);
          if(editedUser) {
            let insertScript =  await this.client2
              .query(`INSERT INTO "user" ("id", "firstName", "fullName","lastName","username", "email", "createdBy",
              "createdAt", "updatedBy","updatedAt","idpCode","idirguid")  
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING *`, editedUser)
              .then(async res => {
              return res.rows[0];
              })
              .catch(async e => {
                await this.client2.query('ROLLBACK');
                this.isError=true;
                winstonLogger.error(e);
                throw new Error(e);
              });
              this.insertRecord['user'].push(insertScript);
          }
        }
      } 
    } catch(e) {
      this.client.connect();
      this.client2.connect();
      winstonLogger.error(e);
      throw new Error(e);
    }
  }
    
  async selectFromUserTable(user){
    let keycloakId = user.id;
    let attributes = user.attributes;
    if(keycloakId && attributes) {
      const query = {
        text:'SELECT * FROM "user" where "keycloakId"=$1',
        values:[keycloakId]
      }
      return await this.client
        .query(query)
        .then(async res => {
          if(res.rows[0]) {
            let fetchedUser = res.rows[0];
            delete fetchedUser.keycloakId;
            if (attributes.idir_user_guid) {
              fetchedUser['idirGUID'] = attributes.idir_user_guid&&attributes.idir_user_guid[0];
            } 
            return Object.values(fetchedUser);   
          } 
        })
        .catch(async e => {
          await this.client2.query('ROLLBACK');
          this.isError = true;
          this.client.connect();
          this.client2.connect();
          winstonLogger.error(e);
          throw new Error(e);
        });
    }    
  }

  async migrateTables(tableNames) {    
    for (let tableName of tableNames) {
      this.insertRecord[tableName]=[];
      let fetchedData = await this.selectFromTable(tableName);
      if(fetchedData) { 
        let rows = this. _extraRows(fetchedData[0]);
        let values=this._extraColumnIndex(fetchedData);
        for (let data of fetchedData) {
          let insertScript =  await this.client2
            .query(`INSERT INTO ${tableName} (${rows})  
            VALUES (${values}) RETURNING *`, Object.values(data))
            .then(async res => { 
              return res.rows[0];
            })
            .catch(async e => {
              await this.client2.query('ROLLBACK');
              this.isError = false;
              this.client.connect();
              this.client2.connect();
              winstonLogger.error(e); 
              throw new Error(e);}); 
              this.insertRecord[tableName].push(insertScript);
        } 
      }
    }
  }
   

  async selectFromTable(tableName) {
    if(tableName) {
      const query= {
        text: `SELECT * from ${tableName}`
      }
    return await this.client
      .query(query)
      .then(async res => {
        if(res.rows){
          return res.rows;   
        } 
      })
      .catch(async e => {
        this.isError=true;
        await this.client2.query('ROLLBACK');
        this.client.connect();
        this.client2.connect();
        winstonLogger.error(e);
        throw new Error(e);
      });
    }
  }

}

module.exports = DatabaseMigration;
