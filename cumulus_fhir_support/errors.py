class RequestError(Exception):
    """
    An error that occurred while trying to process a network request.

    This could be directly from an http response, or during authentication, or during
    post-response processing. Subclasses will let you differentiate.
    """
