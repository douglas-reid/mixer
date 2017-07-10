package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "sdtrace.go",
    ],
    deps = [
        "//adapter/sdtrace/config:go_default_library",
        "//pkg/adapter:go_default_library",
        "@org_golang_google_api//cloudtrace/v1:go_default_library",
        "@org_golang_google_api//option:go_default_library",
        "@org_golang_google_api//support/bundler:go_default_library",
        "@org_golang_google_api//transport:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_x_sync//semaphore:go_default_library",
    ],
)
