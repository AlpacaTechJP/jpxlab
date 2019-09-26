=====
Usage
=====

To use jpxlab in a project::

    import jpxlab

Define SFTP variables::
		
		# SFTP connection info
		host = '10.0.0.1'
		port = 12345
		user = "foo"
		password = "pass"

		# SFTP Folder
		src = '/zips/TheFile.zip'

Define Output Folder::

		# Output Folder in Local Disk
		out_dir = '/output_folder/'

Setting Up the SFTP connection::

		sftp = get_sftp_session(host, port, user, password)

Stream the remote ZIP file into a local HDF5 File::

		out_filename = fetch_and_convert(sftp, src, out_dir)

Resample the Raw HDF5 file by seconds::

		resample(out_filename, out_filename.replace("_raw.h5", ".h5"))