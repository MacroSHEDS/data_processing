function ftgrep {
    grep -Pr --include="*$1" "$2" .
}
