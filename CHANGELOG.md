# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

## [0.8.0] - 2023-05-31
- Update all upstream dependencies

## [0.7.0] - 2023-01-21
- Refactored split code to be more efficient and simpler
- Accept ToSocketAddrs implementing types to make StatsBuilder API more flexible
- Introduce way to generate distributions and histograms without local
  pre-aggregations and reporting them as such

## [0.6.0] - 2022-08-09
### Fixed
- Fix all known cases of split issues

## [0.5.0] - 2022-08-08
### Fixed
- fix more corner cases related to splitting packets

## [0.4.0] - 2022-08-08
### Fixed
- fixed a minor issue in splitting packets when a single metric was longer than max_packet_size

## [0.3.0] - 2022-08-08
### Fixed
- mistaken release

## [0.2.0] - 2022-07-28
### Added
- Introduce the ability to split metric payload in smaller packets that fit MTU


## [0.1.0] - 21/07/2022
### Added
- Initial release
