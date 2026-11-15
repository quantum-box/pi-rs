/// A versioned project resource supplied to an agent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    pub id: String,
    pub version: String,
    pub text: String,
}
