var mqtt = require('mqtt'); 	//https://www.npmjs.com/package/mqtt
// Souscription à tous les topics
var Topic = '#'; 		
// URL du broker MQTT
var Broker_URL = 'mqtt://127.0.0.1';
// URL de la base de données
var Database_URL = '127.0.0.1';

// Options de connexion au broker MQTT
var options = {
	clientId: 'MyMQTT',
	port: 1883,
	username: 'mqtt_user',
	password: 'mqtt_user',	
	keepalive : 60
};

////////////////////////////////////////////////////
///////////////////// MQTT /////////////////////////
////////////////////////////////////////////////////
var client  = mqtt.connect(Broker_URL, options);
client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('error', mqtt_error);
client.on('message', mqtt_messsageReceived);
client.on('close', mqtt_close);

// Fonction de connexion MQTT
function mqtt_connect() {
    console.log("Connexion MQTT");
// Appel de la fonction de souscription
    client.subscribe(Topic, mqtt_subscribe);
};

// Fonction souscription à un topic
function mqtt_subscribe(err, granted) {
    console.log("Subscribed à " + Topic);
    if (err) {console.log(err);}
};

// Fonction de reconnexion
function mqtt_reconnect(err) {
    console.log("Reconnexion MQTT");
    //if (err) {console.log(err);}

// Rappel de la fonction de connexion
	client  = mqtt.connect(Broker_URL, options);
};

// Appel de la fonction d'erreur 
function mqtt_error(err) {
    console.log("Erreur!");
	//if (err) {console.log(err);}
};

function after_publish() {
	//do nothing
};

// Réception d'un message depuis le broker MQTT
function mqtt_messsageReceived(topic, message, packet) {
	var message_str = message.toString(); //convert byte array to string
	message_str = message_str.replace(/\n$/, ''); //remove new line
	//payload syntax: clientID,topic,message

	if (countInstances(message_str) == 3) {
		insert_message(topic, message_str, packet);
		//console.log(message_str);
	} else {
		insert_message_parc(topic,message_str, packet);	
		console.log("Invalide payload");
	}
};

function mqtt_close() {
	//console.log("Fermeture MQTT");
};

////////////////////////////////////////////////////
///////////////////// MYSQL ////////////////////////
////////////////////////////////////////////////////
var mysql = require('mysql'); //https://www.npmjs.com/package/mysql
// Création de la connexion
var connection = mysql.createConnection({
	host: Database_URL,
	user: "user",
	password: "user",
	database: "mydb",
	socketPath: '/var/run/mysqld/mysqld.sock'
});

connection.connect(function(err) {
	if (err) throw err;
	//console.log("Connexion de la base de données !");
});

// Insertion d'une ligne dans la table tbl_message
function insert_message(topic, message_str, packet) {
	// Séparation de la string dans un tableau
	var message_arr = extract_string(message_str); 
	// Récupération du type
	var type = message_arr[0];
	// Récupération du numéro de série
	var serialNumber = message_arr[1];
	// Récupération de la mesure
	var mesure = message_arr[2];
	// Récupération de la localisation
	var location = message_arr[3];

	// Requête SQL pour insérer les informations de la sonde en base de données
	var sql = "INSERT INTO ?? (??,??,??,??,??) VALUES (?,?,?,?,?)";
	var params = ['tbl_message', 'topic','type', 'serialNumber', 'measurement', 'location' , topic, type, serialNumber, mesure, location];
	sql = mysql.format(sql, params);	
	
	connection.query(sql, function (error, results) {
		if (error) throw error;
		console.log("Message ajouté dans la table tbl_message " + message_str);
	}); 

	// Requête SQL pour mettre à jour le statut du capteur (statut = 1 s'il n'est pas en défault)
        var sql = "UPDATE ?? SET ?? = 1 WHERE ?? = ?";
        var params = ['parc', 'status','serialNumber', serialNumber];
        sql = mysql.format(sql, params);

        connection.query(sql, function (error, results) {
                if (error) throw error;
                console.log("Mise à jour de la table parc " + message_str + "\n");
        });

};	

// Insertion d'une ligne dans la table parc
function insert_message_parc(topic, message_str, packet) {
        var message_arr = extract_string(message_str); 

        var type = message_arr[0];
        var serialNumber = message_arr[1];
        var mesure = message_arr[2];
	// Variable utile dans le cas ou l'on transmet la localisation par MQTT (ici location=default)
        var location = message_arr[3];
	
	var status = 0;
	// Récupération de la date d'aujourd'hui
	var dAujourdhui = new Date();
	
	// Requête SQL pour savoir si la sonde en question est déjà en défault (présente dans la base de données)
	var rows_result_select;
	var sql_insert = "SELECT ?? FROM ?? WHERE ?? = ?";
        var params_insert = ['serialNumber', 'parc', 'serialNumber',serialNumber];
	sql_insert = mysql.format(sql_insert, params_insert);
	
        connection.query(sql_insert, function (error, rows, results) {
		if ( rows.length > 0 )  { 
      			var rows_result = rows[0];
			rows_result['serialNumber'];
			rows_result_select = rows_result['serialNumber'];
      			console.log('Resultat :' + rows_result_select);
    		} else {
	      		console.log("Pas de données");
    		}
		if (error){
                         throw error;
                }

                console.log("Demande si la sonde est déjà en défault : ");
 
		// Test si la sonde en question est déjà en défault
		if(rows_result_select == serialNumber){
			console.log("Le default existe déjà");
			 // Requête pour mettre à jour le statut du capteur
        		var sql = "UPDATE ?? SET ?? = 0 WHERE ?? = ?";
        		var params = ['parc', 'status','serialNumber', serialNumber];
        		sql = mysql.format(sql, params);

        		connection.query(sql, function (error, results) {
                	if (error) throw error;
        		        console.log("Mise à jour de la table parc pour défaut " + message_str);
	        	});

		}else{
			// Requête pour insérer une nouvelle ligne de défault dans la table parc
        		var sql = "INSERT INTO ?? (??,??,??,??,??) VALUES (?,?,?,?,?)";
        		var params = ['parc', 'topic','serialNumber','type','dateTimeProduction', 'status', topic, serialNumber, type, dAujourdhui, status];
        		sql = mysql.format(sql, params);

        		connection.query(sql, function (error, results) {
                	if (error) throw error;
                		console.log("Message ajouté dans la table de gestion de parc : ");
        		});	
		}
	});
};



//split a string into an array of substrings
function extract_string(message_str) {
	var message_arr = message_str.split(","); 	
	return message_arr;
};	

//count number of delimiters in a string
var delimiter = ",";
function countInstances(message_str) {
	var substrings = message_str.split(delimiter);
	// console.log("taille:"+ (substrings.length - 1));
	return substrings.length - 1;
};
