from threading import Thread
from os import environ
from cv2 import VideoCapture, CAP_FFMPEG
from time import sleep
from typing import Iterator, Callable
from numpy import ndarray as ndarray


def _rtsp_stream(rtsp_link: str, use_udp: bool = True, retry_if_error: bool = True) -> Iterator[str]:

    capture_options = 'rtsp_transport;'
    capture_options += 'udp' if use_udp else 'tcp'
    environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = capture_options
    cap = VideoCapture(rtsp_link, CAP_FFMPEG)
    if not retry_if_error:
        try:
            while True:
                if cap.isOpened():
                    ret, frame = cap.read()
                    if ret:
                        yield frame
                else:
                    print(f'rtsp-stream is closed. URL:{rtsp_link}')
                    cap.release()
                    return
        except Exception as e:
            print(e)
            cap.release()
    else:
        while True:
            try:
                while True:
                    if cap.isOpened():
                        ret, frame = cap.read()
                        if ret:
                            yield frame
                    else:
                        print(f'rtsp-stream is closed. URL:{rtsp_link} but retrying after 30 seconds')
                        sleep(30)
            except Exception as e:
                print(f'Got exception {e}\n but retrying after 30 seconds')
                sleep(30)
                cap = VideoCapture(rtsp_link,CAP_FFMPEG)
            


def _background_capture(rtsp_link: str, request_list: list[int], feedback_list: list[ndarray], use_udp: bool = True, retry_if_error: bool = True):

    stream = _rtsp_stream(rtsp_link, use_udp, retry_if_error)
    while True:
        while len(request_list) == 0:
            img = next(stream)
        img = next(stream)
        feedback_list.append(img)
        request_list.clear()


def _get_image(request_list: list[int], feedback_list: list[ndarray]) -> ndarray:

    assert len(
        request_list) <= 1, f'You are calling the fetch function multiple times, this wont work'
    assert len(feedback_list) == 0, f'Something is wrong ther feedback_list shouldnt contain a img when this function is called'
    request_list.append(1)
    while True:
        if len(request_list) == 0:
            img = feedback_list[-1]
            feedback_list.clear()
            return img


def run_non_blocking(rtsp_link: str, use_udp: bool = True, retry_if_error: bool = True) -> Callable:
    """Start capturing in a new background thread and return a callable function that fetches the latest frame.
       It is not possible to call the fetch funciton multiple times at the same time just once.
       
       rtsp_link should be your rtsp-stream link like rtsp://admin:admin@192.168.1.1//h264Preview_01_main
       use_udp is a flag to check if you want to connect using udp or tcp
       retry_if_error is a flag to retry if there is no connection or an other error"""
    request_list = []
    feedback_list = []
    capture = Thread(target=_background_capture,
                     args=(rtsp_link, request_list, feedback_list, use_udp, retry_if_error))
    capture.start()

    def fetch() -> ndarray:
        """Returns the latest frame as numpy.ndarray"""
        return _get_image(request_list, feedback_list)

    return fetch


def run_blocking(rtsp_link: str, use_udp: bool = True,    retry_if_error: bool = True) -> Callable:
    """Returns a function that when called fetches a frame from the rtsp-stream.
       It is advisable to always use the non_blocking function and not this one.
       There can be errors if the fetchfunction is called to fast
       
       rtsp_link should be your rtsp-stream link like rtsp://admin:admin@192.168.1.1//h264Preview_01_main
       use_udp is a flag to check if you want to connect using udp or tcp
       retry_if_error is a flag to retry if there is no connection or an other error"""

    stream = _rtsp_stream(rtsp_link, use_udp, retry_if_error)

    def fetch() -> ndarray:
        """Returns the next rtsp frame, if your camera has a queue there can be a significant delay"""
        return next(stream)

    return fetch
