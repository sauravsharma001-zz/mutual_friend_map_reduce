# Map Reduce 

## Calculate mutual friends for all pairs of user
For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy

As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty.


## Calculate top 10 user pair with most number of mutual friends
This is extension of 1st part, where to get top 10 user pair.
Output Format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>


## Given two user, calculate list of the names and states of their mutual friends
Output format:
UserA id, UserB id, list of [states] of their mutual Friends.

## Calculate top 10 user with youngest friend sorted in decreasing order
Output format:  
User A, 1000 Anderson blvd, Dallas, TX, minimum age of direct friends.
