Explainations for the Client operation

There are four parameters for the client command, -rh, -rp, -u, -s. Different combinations of parameters lead to different operation. The -rh and -rp are indispensable for the client to connect to a remote server. In addition to this, users can choose their required parameters.

1. Providing -u, -s means the client executes the login operation with the username and secret
2. providing -u without -s means the client executes the register operation and Client program will generate the secret and print it to console. If register fails, the notification from server will be printed on GUI and client will simply quit.
3. providing -s without -u is meanless. The client program will print a hint and quit.
4. Without -u and -s means the client executes the login operation with anonymous

After connecting to the server and finish the register/login operation, a graphical interface will appear for broadcasting.

When close gui, or press the disconnect button, client program sends logout before terminate.

