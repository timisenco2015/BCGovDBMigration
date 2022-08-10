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
      for(let index in Object.keys(data)) {
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

  //calls this method to start migration
  async migrate(users) {
    try {

      await this.client2.query('BEGIN');

      let tableNames= config.get('tableNames'); 
      //let tableNames= await this.loadTables();
    

      //truncate all tables migration
      await this.truncateTables(tableNames);

      await this.migrateTables(tableNames);

      await this.migrateUserTable(users);

      await this.client2.query('COMMIT');

      if (!this.error) {
        this.client.end();
        this.client2.end();
        winstonLogger.info(this.insertRecord); 
        console.log("----------------------------------------");
        console.log("Connection now closed");
        console.log("----------------------------------------");
      }
    } catch(e) {
      this.client.connect();
      this.client2.connect();
      winstonLogger.error(e);
      
    }
  }

   //this will delete all the data in all the tables. This is to prevent
  // unique constrainsts in all the tables
  async truncateTables(tablesNames) {
    if(tablesNames) {
      for (let tableName of tablesNames) {
        await this.client2
          .query(`TRUNCATE TABLE "${tableName}" CASCADE`)
          .then(async res => {
            winstonLogger.info("All tables truncated");
            return res.rows[0];
          })
          .catch(async e => {
            throw new Error(e);
          });
      }
      console.log("----------------------------");
      console.log("all tables truncated");
      console.log("----------------------------"); 
    }
  }

  async migrateTables(tableNames) {   
    for (let tableName of tableNames) {
      this.insertRecord[tableName]=[];
      let fetchedData = await this.selectFromTable(tableName);
      if(fetchedData.length>0) { 
        let rows = this. _extraRows(fetchedData[0]);
        let values=this._extraColumnIndex(fetchedData[0]);
        for (let data of fetchedData) {
          let insertScript =  await this.client2
            .query(`INSERT INTO "${tableName}" (${rows})  
            VALUES (${values}) RETURNING *`, Object.values(data))
            .then(async res => { 
              return res.rows[0];
            })
            .catch(async e => {
              await this.client2.query('ROLLBACK');
              this.isError = false;
              throw new Error(e);
            }); 
            this.insertRecord[tableName].push(insertScript);
        } 
      }
    }
  }
   
  async selectFromTable(tableName) {
    let query={};
    if(tableName) {
      query= {
        text: `SELECT * from "${tableName}"`
      }
      
    return await this.client
      .query(query)
      .then(async res => {
        if(res.rows){
          return res.rows;   
        } 
      })
      .catch(async (e) => {
        this.isError=true;
        await this.client2.query('ROLLBACK');
        winstonLogger.error(e);
        throw new Error(e);
      });
    }
  }

  /*
  * This method is used to add idirGUID, basicBceidGUID, and businessBceidGUID.
  * then remove keycloakId
  */
  async migrateUserTable(users) {
    try {
      if(users) {
        const query = {
          text:`SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'user'`,
          rowMode: 'array'
        }
        let selectScript =  await this.client2
          .query(query)
          .then(async res => {      
            return res.rows.flat();
          })
          .catch(async e => {
            await this.client2.query('ROLLBACK');
            this.isError=true;
            winstonLogger.error(e);
            throw new Error(e);
          });
              
        let allExists = ['idirGUID','basicBceidGUID','businessBceidGUID'].every(r => selectScript.includes(r));
        if(!allExists) {
          await this.addColumnsToUserTable();
        }
        await this.loadUserFromKeyCloak(users);
        await this.dropKeycloakIdColumnsInUserTable();
      } 
    } catch(e) {
      winstonLogger.error(e);
      throw new Error(e);
    }
  }

  /*
  *This method will add idirGUID, basicBceidGUID, and businessBceidGUID, if they
  * do not already exist
  * */
  async addColumnsToUserTable() {
   
    const query = {
      text:`ALTER TABLE "user"
      ADD "idirGUID" varchar(255),
      ADD "basicBceidGUID" varchar(255),
      ADD "businessBceidGUID" varchar(255)`
    }
    
    return await this.client2
      .query(query)
      .then(async res => {
        return res;
      })
      .catch(async (e) => {
        await this.client2.query('ROLLBACK');
        this.isError=true;
        throw new Error(e);
      });
  }

  async dropKeycloakIdColumnsInUserTable() {
   
    const query = {
      text:`ALTER TABLE "user"
      DROP "keycloakId"` 
    }
    
    return await this.client2
      .query(query)
      .then(async res => {
        return res;
      })
      .catch(async (e) => {
        await this.client2.query('ROLLBACK');
        this.isError=true;
        winstonLogger.error(e);
        throw new Error(e);
      });
  }

  /*
  *
  * this method recieves all users from keycloak and loop through each user,
  * retrieves keycloakId, idir_user_guid, bceid_business_guid, bceid_business_guid, and
  *  then update user table 
  */
  async loadUserFromKeyCloak(users){
    for(let user of users) {
      let selectScript = await this.updateUserTable(user);
    }
  }
    
  async updateUserTable(user){
    let keycloakId = user.id;
    let attributes = user.attributes;
    let setColumn = ''
    if(keycloakId && attributes) {
      if (attributes.idir_user_guid) {
        setColumn = setColumn+`"idirGUID" = '${attributes.idir_user_guid&&attributes.idir_user_guid[0]}'`+","
        //fetchedUser['idirGUID'] = attributes.idir_user_guid&&attributes.idir_user_guid[0];
      }
      if(attributes.bceid_user_guid) {
        setColumn = setColumn+`"basicBceidGUID" = '${attributes.bceid_user_guid&&attributes.bceid_user_guid[0]}'`+","
        //fetchedUser['basicBceidGuid'] = attributes.bceid_user_guid&&attributes.bceid_user_guid[0];
      }
      if(attributes.bceid_business_guid) {
        setColumn = setColumn+`"businessBceidGUID" = '${attributes.bceid_business_guid&&attributes.bceid_business_guid[0]}'`+","
        //fetchedUser['businessBceidGuid'] = attributes.bceid_business_guid&&attributes.bceid_business_guid[0];
      }
      
      setColumn = setColumn.replace(/,\s*$/, "");
      if(setColumn!=='') {
        const query = {
          text:`UPDATE "user" SET ${setColumn} where "keycloakId"=$1`,
          values:[keycloakId]
        }
        return await this.client2
          .query(query)
          .then(async res => {
            if(res.rows[0]) {
              return res.rows[0];   
            } 
          })
          .catch(async(e) => {
            await this.client2.query('ROLLBACK');
            this.isError = true;
            throw new Error(e);
          });
        }
    }    
  }

  /*
  *
  * retrieves all tables from the database
  * Not being used at the moment
  */
  async loadTables() {
   let query= {
      text: `SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public' AND table_type='BASE TABLE'`,
      rowMode: 'array'
    }
    return await this.client
    .query(query)
    .then(async res => {
      if(res.rows){
        return res.rows.flat().filter(function(ele){ 
          return ele !== 'knex_migrations' && ele !== 'knex_migrations_lock'; 
      });   
      } 
    })
    .catch(async e => {
      this.isError=true;
      throw new Error(e);
    });
    
  }

}

module.exports = DatabaseMigration;
