# Wiki

We are planning to use the crate 'rayon' additonally for datarace-free parallel processing. 

## Balancer
The Balancer mainly just forwards requests using functions from the Coordinator (which will be stored as an Arc<Mutex> in order to safely access it) to received servers or directly replies to them. The Balancer mainly just follows the steps from the Project Description by calling the necessary functions. The most extraordinary case are requests that need to be forwarded to a server. For this, we first check if an id is already there, if yes we select it, otherwise using the provided random crate we pick a random server from the active ones. In order to read from the coordinator, we lock it, such that no data race appears, and then request the necessary informations. The most important thing is to handle the three different states of a server correctly (0: Actively Running, 1: In the process of terminating, 2: Fully terminated). In the case of 0, all requests are allowed to be forwarded to the corresponding server. In the case of 1, we do not forward reservation requests. For these, we will select a new random server. We never attempt to make a request to a server with status 2. In this case, we will also select a new random server.

## Estimator
The estimator needs access to the database and to all servers to properly estimate the number of available tickets for each server. As we do not want any data races, this will again be stored as an Arc<Mutex>. We are currently still investigating on ways on how to make it possible to access more things in parallel safely. We also use a HashMap to store the available tickets on each server, such that we do not have to compute everything again in the same iteration of estimation. In our `run` method we first compute the available tickets and then for each server we then just implement the algorithm from the specifcation and using the 'wait' function in each iteration such that everything arrives in time. 
## Coordinator
The coordinator takes mainly care of the main logic of accessing servers. Since there exist multiple functions, we split the explanations into several parts (leaving out trivial getter/setter methods): 

### Data Structures
The coordinator uses Vectors for 'server_list' and 'server_id_list', which both store servers, where the 'server_id_list' is used in case just server IDs need to be returned to not have to access a server object (and potentially having to lock it). Throughout our implementation we assure that active servers will be in the range '[0, no_active_servers]', such that we also don't have to access a server object for simply checking this.
We will also use a HashMap from Server UUID to the index in the corresponding `server_id_list`. 
### 'get_estimator_servers(&self)'
This returns all servers that should be used for estimation, i.e. no server that has fully terminated (`status=2`).
### 'get_random_server(&self)'
Using `rand::thread_rng();` and `rng.gen_range(0..self.no_active_servers) as usize` we can then generate a random index to select a random server, which is needed for Balancing non assigned requests. 
###  'get_server(&self, id)' and 'get_server_mut(&self, id)
In order to not always have to use mutable Servers (which would lower concurrent performance a lot), there exist 2 methods to get_servers, where the first just returns a shared reference and the second a mutable reference. 
### 'scale_to(&mut self, num_servers)'
This is the most complicated part of the coordinator. There exist two basic cases: 
#### Active Servers < Servers to Scale To
Again there exist 2 cases here: 
1) There already exist Server Objects, that were just terminated beforehand, then we simply request them to activate.
2) There do not exist any terminated servers. Then we need to create a new server `Server::new(self.database.clone(), self.reservation_timeout)`. For proper initalization it also needs to be added to all the data_structures we previously explained. 

#### Active Servers  > Servers to Scale To
Here we simply request active_servers - servers_to_scale_to servers to terminate.  
## Lib 
Here we simply have to complete the launch function. We start creating the needed servers with the `config.inital_servers' value using the 'scale_to' method of the 'coordinator'. [To be completed]
## Server

### Deactivation
For our server we use the variable `status` as a state counter to indicate if the server is active (`status=0`), in the process of termianting (`status=1`) or has fully terminated (`status=2`). The function `deactivate` will set the status to 1 first, as cancellation/buying requests should be still processed. The current tickets should still be cleared by being deallocated to the database (using the respective function with a lock on the database) first. If the requests are empty we immediately set the status to 2. 

### Handling Requests
Before handling requests at all we check if the timeout has occured using `time.elepased().as_secs() > self.timeout`. If yes and the server is active, the ticket will be returned to the current list of tickets of the server. If yes and the server is terminating then the ticket will be given to the database. We also check before handling requests if no reservations are left and hence also no future buy/cancel requests for new tickets, and just return the tickets to the database.
#### Num Avaialble Ticketes
This is just a getter request, that is too trivial too explain
#### Reserve Ticket
We first set the current request id to 


#### Buy Ticket

#### Abort Purchase