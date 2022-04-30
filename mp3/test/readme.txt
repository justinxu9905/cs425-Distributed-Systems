# Instructions for running the test
Place your code folder that contains the compiled executable here and rename your folder to 'src'
Make sure your code can run by executing './server <branch_name> <config_file>' and './client <client_id> <config_file>' as stated in the MP3 documentation
Run your code by executing './run.sh'
If you see no output after printing "Difference between your output and expected output:", that means your code passes this basic test.
Otherwise, check what's the difference between your output file and the expected output file and debug your code accordingly.
Ideally, each client program should run much faster than 5 seconds. This testing scripts will timeout if your client code runs more than 5s.

# Test cases
input1.txt will deposit to both account A.a and B.b, and your client output should match the content in expected1.txt
input2.txt will check the balance of both account A.a and B.b, and your client output should match the content in expected2.txt