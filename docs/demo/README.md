Janitor Demo (GIF)
==================

This folder describes how to record and generate the CLI demo GIF referenced in the README.

Option A: asciinema + agg (recommended)
--------------------------------------
1) Install tools (Homebrew):
```
brew install asciinema
brew install agg
```
2) Record a short session:
```
asciinema rec -c "janitor --config examples/config/rules.json --chunk-size 5000 --expected-rows 150000 --verbose" demo.cast
```
3) Render to GIF:
```
agg --font-size 14 --theme github-dark demo.cast ../assets/demo.gif
```

Option B: Terminalizer
----------------------
1) Install:
```
npm i -g terminalizer
```
2) Record and render:
```
terminalizer record demo
terminalizer render demo -o ../assets/demo.gif
```

Tips
----
- Keep the recording ~10–15 seconds; show one run with a visible config path and a short summary.
- Resize the terminal to a compact width (80–100 cols) for readability.
- Commit the resulting GIF at `docs/assets/demo.gif`.
