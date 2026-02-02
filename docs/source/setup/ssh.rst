.. role:: python(code)
    :language: python

SSH
############################################################

UNIX machines (Mac/Linux) will look for SSH an configuration file at:

.. code-block:: bash

    $ cat ~/.ssh/config

If the file doesn't yet exist, create one like so:

.. code-block:: bash

    $ mkdir -p ~/.ssh
    $ touch ~/.ssh/config

If you normally connect to ssh like so:

.. code-block:: bash

    $ ssh username@remote_machine

Then adding the following to the config, choose a name that makes sense to you...

.. code-block:: text

    host my_custom_name
        HostName remote_machine
        User username

...will let you do this: 

.. code-block:: bash

    $ ssh my_custom_name

For passswords, we can create a pair of ssh keys. Follow the prompts, do not
set a password for the file, and name it something related to the machine.
I've named mine "my_key", but you should pick a better name.

.. code-block:: bash

    $ ssh-keygen -a 128 -t ed25519

One of the files will end in .pub.

.. code-block:: bash

    $ cat my_key.pub

This is your public key. The other is your private key, guard it with your life.
Sometimes, it is necessary to change the permissions of the private key.

.. code-block:: bash

    $ chmod 600 my_key

Connect to the remote machine and add the the public key (it should only be one line) to 
the authorized_keys file.

.. code-block:: bash

    # on the remote machine
    $ echo "the key" >> ~/.ssh/authorized_keys

Adding the following config will use the ssh key instead of the password.

.. code-block:: text

    host my_custom_name
        ...
        PreferredAuthentications publickey
        IdentityFile ~/.ssh/my_key

Now you should be able to connect without typing the password as well.

.. code-block:: bash

    $ ssh my_custom_name

Finally, the following lines tell new connections to use existing connections if possible.
This makes it so you only have to do 2FA once.

.. code-block:: text

    host my_custom_name
        ...
        ControlMaster auto
        ControlPath ~/.ssh/live_connections/%h_%p_%r

Additional remote machines can be added and settings can be applied to multiple hosts. The
following sets both "my_custom_1" and "my_custom_2" to use control master.

.. code-block:: text

    host my_custom*
        ControlMaster auto
        ControlPath ~/.ssh/live_connections/%h_%p_%r

    host my_custom_1
        ...

    host my_custom_2
        ...

Your final config file should look something like this

.. code-block:: text

    host my_custom*
        ControlMaster auto
        ControlPath ~/.ssh/live_connections/%h_%p_%r

    host my_custom_name
        HostName remote_machine
        User username
        PreferredAuthentications publickey
        IdentityFile ~/.ssh/my_key
