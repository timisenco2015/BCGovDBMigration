const DatabaseMigration = require('./dataConnection/databaseMigration');
const databaseMigration = new DatabaseMigration();
const config = require('config');

import ('@keycloak/keycloak-admin-client').then(KcAdminClient=>{
  const migration = async()=> {
    const kcAdminClient = new KcAdminClient.default({
     baseUrl: config.get('keycloak.serverUrl'),
     realmName: config.get('keycloak.realm'),
    });
    await kcAdminClient.auth({
      'grantType': 'client_credentials',
      clientId: config.get('keycloak.clientId'),
      clientSecret: config.get('keycloak.clientSecret'),
    });
 
    const users = await kcAdminClient.users.find();
    let reducedUsers = users.map(user=>({'id':user.id, 'attributes':user.attributes}));
    await databaseMigration.migrate(reducedUsers);
    
   
  }
  migration();
});
 