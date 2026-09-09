// Copyright (c) 2025, b»robotized by Stogl Robotics
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <rclcpp/rclcpp.hpp>
#include "test_gpio_tool_controller.hpp"

// Regression test for: a goal used to be accepted and start a tool-state transition even
// though the controller was never activated. Since `update()` only runs while the controller
// is active, such a goal could never make progress - it wedged forever and cancels were never
// serviced either. `process_engaging_request`/`process_reconfigure_request` now reject any
// request unless the controller is active.
TEST_F(GpioToolControllerTest, GoalRejectedWhileInactive)
{
  SetUpController(
    "test_gpio_tool_controller",
    {rclcpp::Parameter("possible_engaged_states", possible_engaged_states)});
  setup_parameters();

  ASSERT_EQ(
    controller_->on_configure(rclcpp_lifecycle::State()),
    controller_interface::CallbackReturn::SUCCESS);

  // The controller is configured but never activated - on_activate() is intentionally not
  // called, reproducing bring-up or an error drop to 'inactive'.

  const auto response = controller_->process_engaging_request(
    gpio_tool_controller::ToolAction::ENGAGING, controller_->params_.engaged.name);

  // The request is rejected because the controller is not active - no transition starts.
  EXPECT_FALSE(response.success);
  EXPECT_EQ(controller_->tool_action(), gpio_tool_controller::ToolAction::IDLE);
  EXPECT_EQ(controller_->tool_transition(), control_msgs::msg::GPIOToolTransition::IDLE);
}
