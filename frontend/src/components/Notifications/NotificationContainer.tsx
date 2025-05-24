import { Box, Stack } from "@chakra-ui/react"
import { useEffect, useState } from "react"
import {
  MinioObjectCreatedEvent,
  useSSEUpdates
} from "../../hooks/useSSEUpdates"
import { FileUploadNotification } from "./FileUploadNotification"
import { UtilsService } from "../../client"
import useCustomToast from "../../hooks/useCustomToast"

interface NotificationItem {
  id: string
  fileName: string
  bucketName: string
  fileSize: number
  timestamp: string
}

const cleanUrl = (url: string | null): string | null => {
  if (!url) return null;
  const protocolSeparator = "://";
  const protocolIndex = url.indexOf(protocolSeparator);
  if (protocolIndex > -1) {
    const protocol = url.substring(0, protocolIndex + protocolSeparator.length);
    let path = url.substring(protocolIndex + protocolSeparator.length);
    path = path.replace(/\/\/+/g, '/'); // Replace multiple slashes with a single slash
    return protocol + path;
  } else {
    console.warn("[NotificationContainer] URL string does not contain '://', attempting general cleanup:", url);
    return url.replace(/\/\/+/g, '/');
  }
};

export const NotificationContainer = () => {
  const [notifications, setNotifications] = useState<NotificationItem[]>([])
  const [sseUrl, setSseUrl] = useState<string | null>(null)
  const { showSuccessToast, showErrorToast } = useCustomToast()

  useEffect(() => {
    const fetchSSEUrl = async () => {
      let rawUrlToClean: string | null = null;
      try {
        const envSseUrl = import.meta.env.VITE_SSE_URL;
        if (envSseUrl && typeof envSseUrl === 'string') {
          console.log("[NotificationContainer] Using SSE URL from VITE_SSE_URL:", envSseUrl);
          rawUrlToClean = envSseUrl;
        } else {
          console.log("[NotificationContainer] VITE_SSE_URL not found or invalid, falling back to UtilsService.getSseUrl()");
          const fetchedData = await UtilsService.getSseUrl()
          console.log("[NotificationContainer] Fetched data from getSseUrl:", fetchedData);
          console.log("[NotificationContainer] Type of fetched data:", typeof fetchedData);

          if (typeof fetchedData === 'string') {
            rawUrlToClean = fetchedData;
          } else if (typeof fetchedData === 'object' && fetchedData !== null && 'url' in fetchedData && typeof (fetchedData as any).url === 'string') {
            console.warn("[NotificationContainer] getSSEUrl returned an object with a URL property. Using fetchedData.url.");
            rawUrlToClean = (fetchedData as any).url;
          } else if (typeof fetchedData === 'object' && fetchedData !== null) {
            console.error("[NotificationContainer] getSSEUrl returned an unexpected object structure:", JSON.stringify(fetchedData));
            const dataObject = fetchedData as Record<string, any>;
            for (const key in dataObject) {
              if (Object.prototype.hasOwnProperty.call(dataObject, key) && typeof dataObject[key] === 'string' && (dataObject[key].startsWith('http://') || dataObject[key].startsWith('https://'))) {
                console.warn(`[NotificationContainer] Fallback: Found potential URL in object property '${key}': ${dataObject[key]}`);
                rawUrlToClean = dataObject[key];
                break;
              }
            }
            if (!rawUrlToClean) {
               setTimeout(() => showErrorToast("Received invalid SSE URL format from server (UtilsService)."), 0);
               return;
            }
          } else {
            console.error("[NotificationContainer] getSSEUrl returned unexpected data type:", fetchedData);
            setTimeout(() => showErrorToast("Could not retrieve SSE URL (UtilsService)."), 0);
            return;
          }
        }

        const actualUrl = cleanUrl(rawUrlToClean);
        console.log("[NotificationContainer] Final cleaned URL for SSE:", actualUrl);

        if (actualUrl) {
          setSseUrl(actualUrl)
          console.log(`[NotificationContainer] SSE URL successfully set to: ${actualUrl}`)
        } else {
          console.error("[NotificationContainer] No valid SSE URL could be determined.");
          setTimeout(() => showErrorToast("Could not determine SSE URL for real-time updates."), 0);
        }

      } catch (error) {
        console.error("[NotificationContainer] Error in fetchSSEUrl logic:", error)
        setTimeout(() => showErrorToast("Failed to configure real-time updates service."), 0);
      }
    }

    fetchSSEUrl()
  }, [showErrorToast])

  const {
    isConnected,
    error,
    registerHandler,
    connect
  } = useSSEUpdates()

  useEffect(() => {
    if (isConnected) {
      const timerId = setTimeout(() => {
        showSuccessToast("You will now receive real-time file upload notifications");
      }, 0);
      return () => clearTimeout(timerId);
    }
  }, [isConnected, showSuccessToast]);

  useEffect(() => {
    if (error) {
      const timerId = setTimeout(() => {
        showErrorToast(error);
      }, 0);
      return () => clearTimeout(timerId);
    }
  }, [error, showErrorToast]);

  useEffect(() => {
    if (!sseUrl) return

    const handleFileUpload = (event: MinioObjectCreatedEvent) => {
      if (event.event_type === "minio_object_created" && event.data) {
        const { object_key, bucket_name, object_size, event_time } = event.data
        const notificationId = `${bucket_name}-${object_key}-${Date.now()}`
        setNotifications(prev => [
          {
            id: notificationId,
            fileName: object_key,
            bucketName: bucket_name,
            fileSize: object_size,
            timestamp: event_time
          },
          ...prev
        ].slice(0, 5))
      }
    }

    const unregister = registerHandler("minio_object_created", handleFileUpload)
    return unregister
  }, [registerHandler, sseUrl])

  // Connect when sseUrl is available
  useEffect(() => {
    if (sseUrl) {
      connect(sseUrl)
    }
  }, [sseUrl, connect])

  const removeNotification = (id: string) => {
    setNotifications(prev => prev.filter(notification => notification.id !== id))
  }

  return (
    <Box
      position="fixed"
      bottom="20px"
      right="20px"
      zIndex="toast"
      maxW="sm"
    >
      <Stack gap={2}>
        {notifications.map(notification => (
          <FileUploadNotification
            key={notification.id}
            fileName={notification.fileName}
            bucketName={notification.bucketName}
            fileSize={notification.fileSize}
            timestamp={notification.timestamp}
            onClose={() => removeNotification(notification.id)}
          />
        ))}
      </Stack>
    </Box>
  )
}
