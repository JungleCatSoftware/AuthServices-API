# AuthServices-API
Code for the AuthServices API back-end

## Endpoints
Documentation for the HTTP API endpoints of the service.

### /sessions/\<user\>@\<org\>
#### GET
View user's sessions. Requires being logged in as the requested user.

##### Parameters
 - key: Valid session key

##### Returns
 - 200: List of sessions containing:
    - sessionid: ID of the session
    - startdate: Date the session was first opened
    - lastupdate: Date of the last time the session was refreshed
 - 400: Request missing arguments. See message for more info.
 - 401: Invalid key or key expired.
 - 403: Key valid, but user associated with the key is not allowed to access this resource.

#### POST
Open session for a user

##### Parameters
 - password: A PBKDF2 hash of the new password using "username@org" as the salt and a count of 10,000

##### Returns
 - 200: Password valid
    - id: Session's Id
    - key: Session's key
 - 400: Incorrect password
 - 404: Invalid user

### /users
#### POST
Create a new user.

##### Parameters
 - username: Name of the user to create.
 - org: Name of organization to create the user on.
 - email: User's email address. Used for sending password resets.
 - parentuser (optional): The parent user for user being created. Must follow the form "user@org".

##### Returns
 - 200: User successfully created.
 - 400: User was not created. User may already exist, Org may be closed or non-existent. See message for specific details
 - 500: Something unexpected happened. The user may not have been created.

### /users/\<user\>@\<org\>
#### GET
Retrieve basic user information.

##### Parameters
None.

##### Returns
 - 200: Object containing username, org, parentuser, and create date of the requested user.
 - 400: Request returned more than one result. This should not happen.
 - 404: No user matching the request could be found.
 - 500: The request resulted in an error and could not be completed.

### /users/\<user\>@\<org\>/completepasswordreset
#### POST
Complete a password reset for a user from a previous request.

##### Parameters
 - resetid: The UUID of the reset request
 - password: A PBKDF2 hash of the new password using "username@org" as the salt and a count of 10,000

##### Returns
 - 200: Password for the user was successfully updated.
 - 400: The request is invalid. See message for specific details.
 - 500: An error occurred whie processing the request.


### /users/\<user\>@\<org\>/requestpasswordreset
#### POST
Request a password reset for a user.

##### Parameters
None.

##### Returns
 - 200: Password reset was successfully generated for the user.
 - 400: No such user exists. No reset request was generated.
 - 500: An error occured creating the reset request.
