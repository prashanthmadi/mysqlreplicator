###Mysql Replicator###


####Usage####
```javascript
var mysqlreplicator = require('mysqlreplicator');


mysqlreplicator({
	host: '',
	user: '',
	password: '',
	database: '',
},{
	host: '',
	user: '',
	password: '',
	database: '',
	multipleStatements: true
},function(err,data){
	if(err){
		console.log(err);	
	}
	process.exit();
});
```
