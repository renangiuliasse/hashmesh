#[cfg(test)]
mod tests {
    use lib::SoftwareVersion;

    #[test]
    fn projectversion() {
        let _ = SoftwareVersion::project_version();
    }
}
