fn main() {
    cxx_build::bridge("src/lib.rs")
        .file("src/main.cpp")
        .flag_if_supported("-std=c++17")
        .compile("bendsql_cpp");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/main.cpp");
}
