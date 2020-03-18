(declare-project
  :name "db"
  :description "A humble database library for janet"
  :dependencies ["https://github.com/janet-lang/path"]
  :author "Sean Walker"
  :license "MIT"
  :url "https://github.com/joy-framework/db"
  :repo "git+https://github.com/joy-framework/db")

(declare-binscript
  :main "db")

(declare-source
  :source @["src/db" "src/db.janet"])