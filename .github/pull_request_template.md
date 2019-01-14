*** **CHECKLIST FOR PULL REQUESTS!!!** ***

This change is:

- [ ] A documentation / typographical error fix
	- Good to go, no issue or tests are needed
- [ ] A short code fix
	- [ ] I have filed [a bug report](https://github.com/sqlalchemy/sqlalchemy/issues) which includes
	      a complete [MCVE](http://stackoverflow.com/help/mcve) illustrating in simple code form
	      the expected behavior and how the library deviates from it
	- [ ] My git commit includes a `Fixes: #<issue number>` comment with the above bug report number
	- [ ] My code fix includes a unit test, which is typically based on the MCVE I did above
		- [ ] I then **ran the tests**, using the instructions at [README.unittests.rst](https://github.com/sqlalchemy/sqlalchemy/blob/master/README.unittests.rst)
	- [ ] I don't know how to write a test for this.    In which case please FILE A BUG ONLY.
- [ ] A new feature implementation
	- [ ] I have filed [a bug report](https://github.com/sqlalchemy/sqlalchemy/issues) which includes
	      a complete description of the new behavior, including an a Python code example that illustrates
	      how the feature would work.
	- [ ] The new feature request was approved by SQLAlchemy maintainers who requested that I submit
	      a pull request.
	- [ ] My git commit includes a `Fixes: #<issue number>` comment with the above bug report number
	- [ ] My code fix includes unit tests that assert the behavior is as expected.
		- [ ] I then **ran the tests**, using the instructions at [README.unittests.rst](https://github.com/sqlalchemy/sqlalchemy/blob/master/README.unittests.rst)
	- [ ] I don't know how to write the tests.  in which case please GET HELP FROM THE MAINTAINERS
	      on the above mentioned issue how tests for this feature should be structured.

Note that **we do not accept one-liner code fix pull requests with no tests**.  Code that is not
tested is itself **a bug**.   We will help you fix any problem you have, as long as you produce
[issue reports](https://github.com/sqlalchemy/sqlalchemy/issues) that include code samples, stack traces, and most preferably complete standalone test cases.

**Have a nice day!**
