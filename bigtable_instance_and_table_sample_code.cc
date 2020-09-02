// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "google/cloud/bigtable/examples/bigtable_examples_common.h"
#include "google/cloud/bigtable/table_admin.h"
#include "google/cloud/bigtable/instance_admin.h"
#include "google/cloud/internal/getenv.h"
#include "google/cloud/testing_util/crash_handler.h"
#include <sstream>

namespace {

using google::cloud::bigtable::examples::Usage;

void CreateInstance(google::cloud::bigtable::InstanceAdmin instance_admin,
                    std::vector<std::string> const& argv) {
  //! [create instance] [START bigtable_create_prod_instance]
  namespace cbt = google::cloud::bigtable;
  using google::cloud::future;
  using google::cloud::StatusOr;
  [](cbt::InstanceAdmin instance_admin, std::string const& instance_id,
     std::string const& zone) {
    std::string display_name("Put description here");
    std::string cluster_id = instance_id + "-c1";
    auto cluster_config = cbt::ClusterConfig(zone, 3, cbt::ClusterConfig::HDD);
    cbt::InstanceConfig config(instance_id, display_name,
                               {{cluster_id, cluster_config}});
    config.set_type(cbt::InstanceConfig::PRODUCTION);

    future<StatusOr<google::bigtable::admin::v2::Instance>> instance_future =
        instance_admin.CreateInstance(config);
    // Show how to perform additional work while the long running operation
    // completes. The application could use future.then() instead.
    std::cout << "Waiting for instance creation to complete " << std::flush;
    instance_future.wait_for(std::chrono::seconds(1));
    std::cout << '.' << std::flush;
    auto instance = instance_future.get();
    if (!instance) throw std::runtime_error(instance.status().message());
    std::cout << "DONE, details=" << instance->DebugString() << "\n";
  }
  //! [create instance] [END bigtable_create_prod_instance]
  (std::move(instance_admin), argv.at(0), argv.at(1));
}

// Create table
void CreateTable(google::cloud::bigtable::TableAdmin const& admin,
                 std::vector<std::string> const& argv) {
  //! [create table] [START bigtable_create_table]
  namespace cbt = google::cloud::bigtable;
  using google::cloud::StatusOr;
  [](cbt::TableAdmin admin, std::string const& table_id) {
    StatusOr<google::bigtable::admin::v2::Table> schema = admin.CreateTable(
        table_id,
        cbt::TableConfig({{"stats_summary", cbt::GcRule::MaxNumVersions(10)}}, {}));
    if (!schema) throw std::runtime_error(schema.status().message());
    std::cout << "Table successfully created: " << schema->DebugString()
              << "\n";
  }
  //! [create table] [END bigtable_create_table]
  (std::move(admin), argv.at(0));
}

// Preparing table data by inserting table rows
void PrepareReadSamples(google::cloud::bigtable::Table table) {
  namespace cbt = google::cloud::bigtable;
  cbt::BulkMutation bulk;

  std::string const column_family_name = "stats_summary";
  auto const timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());

  bulk.emplace_back(
      cbt::SingleRowMutation("phone#abc123#20200903",
                             {cbt::SetCell(column_family_name, "connected_cell",
                                           timestamp, std::int64_t{1}),
                              cbt::SetCell(column_family_name, "connected_wifi",
                                           timestamp, std::int64_t{1}),
                              cbt::SetCell(column_family_name, "os_build",
                                           timestamp, "PQ2A.190405.003")}));
  bulk.emplace_back(
      cbt::SingleRowMutation("phone#abc123#20200904",
                             {cbt::SetCell(column_family_name, "connected_cell",
                                           timestamp, std::int64_t{1}),
                              cbt::SetCell(column_family_name, "connected_wifi",
                                           timestamp, std::int64_t{1}),
                              cbt::SetCell(column_family_name, "os_build",
                                           timestamp, "PQ2A.190405.003")}));
  bulk.emplace_back(
      cbt::SingleRowMutation("phone#abc123#20200905",
                             {cbt::SetCell(column_family_name, "connected_cell",
                                           timestamp, std::int64_t{0}),
                              cbt::SetCell(column_family_name, "connected_wifi",
                                           timestamp, std::int64_t{1}),
                              cbt::SetCell(column_family_name, "os_build",
                                           timestamp, "PQ2A.190406.000")}));

  std::vector<cbt::FailedMutation> failures = table.BulkApply(std::move(bulk));
  if (failures.empty()) {
    std::cout << "All rows successfully written\n";
    return;
  }
  std::cerr << "The following mutations failed:\n";
  for (auto const& f : failures) {
    std::cerr << "index[" << f.original_index() << "]=" << f.status() << "\n";
  }
  throw std::runtime_error(failures.front().status().message());
}

void ReadRow(google::cloud::bigtable::Table table,
             std::vector<std::string> const& argv) {
  //! [START bigtable_reads_row]
  namespace cbt = google::cloud::bigtable;
  using google::cloud::StatusOr;
  [](google::cloud::bigtable::Table table, std::string const& row_key) {
    StatusOr<std::pair<bool, cbt::Row>> tuple =
        table.ReadRow(row_key, cbt::Filter::PassAllFilter());
    if (!tuple) throw std::runtime_error(tuple.status().message());
    if (!tuple->first) {
      std::cout << "Row " << row_key << " not found\n";
      return;
    }
    PrintRow(tuple->second);
  }
  //! [END bigtable_reads_row]
  (std::move(table), argv.at(0));
}

void DropAllRows(google::cloud::bigtable::TableAdmin const& admin,
                 std::vector<std::string> const& argv) {
  //! [drop all rows]
  // [START bigtable_truncate_table] [START bigtable_delete_rows]
  namespace cbt = google::cloud::bigtable;
  [](cbt::TableAdmin admin, std::string const& table_id) {
    google::cloud::Status status = admin.DropAllRows(table_id);
    if (!status.ok()) throw std::runtime_error(status.message());
    std::cout << "All rows successfully deleted\n";
  }
  // [END bigtable_truncate_table] [END bigtable_delete_rows]
  //! [drop all rows]
  (std::move(admin), argv.at(0));
}

// Delete table
void DeleteTable(google::cloud::bigtable::TableAdmin const& admin,
                 std::vector<std::string> const& argv) {
  //! [delete table] [START bigtable_delete_table]
  namespace cbt = google::cloud::bigtable;
  [](cbt::TableAdmin admin, std::string const& table_id) {
    google::cloud::Status status = admin.DeleteTable(table_id);
    if (!status.ok()) throw std::runtime_error(status.message());
    std::cout << "Table successfully deleted\n";
  }
  //! [delete table] [END bigtable_delete_table]
  (std::move(admin), argv.at(0));
}

void DeleteInstance(google::cloud::bigtable::InstanceAdmin instance_admin,
                    std::vector<std::string> const& argv) {
  //! [delete instance] [START bigtable_delete_instance]
  namespace cbt = google::cloud::bigtable;
  [](cbt::InstanceAdmin instance_admin, std::string const& instance_id) {
    google::cloud::Status status = instance_admin.DeleteInstance(instance_id);
    if (!status.ok()) throw std::runtime_error(status.message());
    std::cout << "Successfully deleted the instance " << instance_id << "\n";
  }
  //! [delete instance] [END bigtable_delete_instance]
  (std::move(instance_admin), argv.at(0));
}


void RunAll(std::vector<std::string> const& argv) {
  namespace examples = ::google::cloud::bigtable::examples;
  namespace cbt = google::cloud::bigtable;

  if (!argv.empty()) throw Usage{"auto"};
  if (!examples::RunAdminIntegrationTests()) return;
  examples::CheckEnvironmentVariablesAreSet({
      "GOOGLE_CLOUD_PROJECT",
      "GOOGLE_CLOUD_CPP_BIGTABLE_TEST_SERVICE_ACCOUNT",
      "GOOGLE_CLOUD_CPP_BIGTABLE_TEST_ZONE_A",
      "GOOGLE_CLOUD_CPP_BIGTABLE_TEST_ZONE_B",
  });
  auto const project_id =
      google::cloud::internal::GetEnv("GOOGLE_CLOUD_PROJECT").value();
  auto const service_account =
      google::cloud::internal::GetEnv(
          "GOOGLE_CLOUD_CPP_BIGTABLE_TEST_SERVICE_ACCOUNT")
          .value();
  auto const zone_a =
      google::cloud::internal::GetEnv("GOOGLE_CLOUD_CPP_BIGTABLE_TEST_ZONE_A")
          .value();

  cbt::InstanceAdmin admin(
      cbt::CreateDefaultInstanceAdminClient(project_id, cbt::ClientOptions{}));

  cbt::TableAdmin admin_table(
      cbt::CreateDefaultAdminClient(project_id, cbt::ClientOptions{}),
      instance_id);

  auto generator = google::cloud::internal::DefaultPRNG(std::random_device{}());
  auto const instance_id = examples::RandomInstanceId("exin-", generator);

  std::cout << "\nRunning CreateInstance() example" << std::endl;
  CreateInstance(admin, {instance_id, zone_a});

  auto table_1 = admin_table.CreateTable(
      table_id_1, cbt::TableConfig({{"stats_summary", cbt::GcRule::MaxNumVersions(10)}}, {}));
  if (!table_1) throw std::runtime_error(table_1.status().message());

 // std::cout << "\nRunning GetOrCreateTable() example [1]" << std::endl;
  //GetOrCreateTable(admin, {table_id_1});

  google::cloud::bigtable::Table table_2(
      google::cloud::bigtable::CreateDefaultDataClient(
          admin_table.project(), admin_table.instance_id(),
          google::cloud::bigtable::ClientOptions()),
      table_id_1);


  std::cout << "Preparing data for read examples" << std::endl;
  PrepareReadSamples(table_2);

  std::cout << "Running ReadRow" << std::endl;
  ReadRow(table_2, {"phone#abc123#20200903"});

  std::cout << "\nRunning DropAllRows() example" << std::endl;
  DropAllRows(admin, {table_id_1});

  std::cout << "\nRunning DeleteTable() example" << std::endl;
  DeleteTable(admin, {table_id_1});

  std::cout << "\nRunning DeleteInstance() example" << std::endl;
  DeleteInstance(admin, {instance_id});
}

}  // anonymous namespace ends

int main(int argc, char* argv[]) {
  google::cloud::testing_util::InstallCrashHandler(argv[0]);

  namespace examples = google::cloud::bigtable::examples;
  examples::Example example({
      examples::MakeCommandEntry("create-instance", {"<instance-id>", "<zone>"},
                                 CreateInstance),
      examples::MakeCommandEntry("create-table", {"<table-id>"}, CreateTable),
      examples::MakeCommandEntry("read-row", {"<row-key>"}, ReadRow),
      examples::MakeCommandEntry("drop-all-rows", {"<table-id>"}, DropAllRows),
      examples::MakeCommandEntry("delete-table", {"<table-id>"}, DeleteTable),
      {"auto", RunAll},
  });

  return example.Run(argc, argv);
}
