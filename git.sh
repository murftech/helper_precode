git init
git add .
git commit -m "initial commit: precode package (shared dr/dc/rw/ds helpers, pulled from my-massive-app + my-massive-diary's drifted copies)"
gh repo create helper_precode --public --source=. --remote=origin --push

git remote -v
gh repo view --web
