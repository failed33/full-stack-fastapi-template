import {
  Box,
  Flex,
  Text,
} from "@chakra-ui/react"
import { useState, useEffect } from "react"
import { FiFile } from "react-icons/fi"
import { CloseButton } from "../ui/close-button"

interface FileUploadNotificationProps {
  fileName: string
  bucketName: string
  fileSize: number
  timestamp: string
  onClose: () => void
}

const formatFileSize = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`
  else if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  else if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  else return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`
}

const formatTimestamp = (isoString: string): string => {
  try {
    const date = new Date(isoString)
    return date.toLocaleString()
  } catch (e) {
    return isoString
  }
}

export const FileUploadNotification = ({
  fileName,
  bucketName,
  fileSize,
  timestamp,
  onClose
}: FileUploadNotificationProps) => {
  const [isVisible, setIsVisible] = useState(true)

  const handleClose = () => {
    setIsVisible(false)
    onClose()
  }

  // Auto-hide notification after 10 seconds
  useEffect(() => {
    const timer = setTimeout(() => {
      handleClose()
    }, 10000)
    return () => clearTimeout(timer)
  }, [])

  if (!isVisible) return null

  return (
    <Box
      bg="blue.500"
      color="white"
      borderRadius="md"
      boxShadow="md"
      mb={4}
      position="relative"
      p={4}
    >
      <CloseButton
        position="absolute"
        top={2}
        right={2}
        size="sm"
        onClick={handleClose}
      />
      <Box mb={2}>
        <Text fontWeight="bold">New File Uploaded</Text>
      </Box>
      <Box>
        <Flex align="center" mb={1}>
          <Box mr={2}>
            <FiFile />
          </Box>
          <Text
            fontWeight="bold"
            fontSize="sm"
            title={fileName}
            maxW="200px"
            overflow="hidden"
            textOverflow="ellipsis"
            whiteSpace="nowrap"
          >
            {fileName}
          </Text>
        </Flex>
        <Text fontSize="xs">Bucket: {bucketName}</Text>
        <Text fontSize="xs">Size: {formatFileSize(fileSize)}</Text>
        <Text fontSize="xs">Time: {formatTimestamp(timestamp)}</Text>
      </Box>
    </Box>
  )
}
