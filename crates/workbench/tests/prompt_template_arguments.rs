use pi_rs_workbench::prompt_template_arguments::expand_arguments;

#[test]
fn expands_quotes_defaults_slices_and_literal_argument_placeholders() {
    assert_eq!(
        expand_arguments("$1|$2|$@|${3:-fallback}|${@:2:2}", "'one two' \"\" three"),
        "one two||one two  three|three| three"
    );
    assert_eq!(expand_arguments("$1 $2 ${2:-none}", "'$2'"), "$2  none");
    assert_eq!(
        expand_arguments("${@:0} ${ARGUMENTS:-default}", ""),
        " default"
    );
}
