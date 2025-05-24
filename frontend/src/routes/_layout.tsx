import { Flex } from "@chakra-ui/react"
import { Outlet, createFileRoute, redirect } from "@tanstack/react-router"
import { Box } from "@chakra-ui/react"
import { Suspense } from "react"

import Navbar from "@/components/Common/Navbar"
import Sidebar from "@/components/Common/Sidebar"
import { isLoggedIn } from "@/hooks/useAuth"
import { NotificationContainer } from "../components/Notifications/NotificationContainer"

export const Route = createFileRoute("/_layout")({
  component: Layout,
  beforeLoad: async () => {
    if (!isLoggedIn()) {
      throw redirect({
        to: "/login",
      })
    }
  },
})

function Layout() {
  return (
    <Flex h="100vh">
      <Sidebar />
      <Flex flexDir="column" flex="1" overflowY="auto">
        <Navbar />
        <Box as="main" flex="1" p={4}>
          <Suspense fallback={<div>Loading...</div>}>
            <Outlet />
          </Suspense>
        </Box>
      </Flex>
      <NotificationContainer />
    </Flex>
  )
}

export default Layout
