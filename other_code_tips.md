# Code tips
I wrote the tips or solutions on a variety of problems I encounted while learning this course. 

### How to remove the file also from git history 
It's not ideal circumstance, but in case you make mistake!
```
git filter-branch --index-filter 'git rm -rf --cached --ignore-unmatch path_to_file' HEAD
```
```
git push --force
```